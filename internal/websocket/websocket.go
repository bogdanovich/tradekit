package websocket

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bogdanovich/tradekit"
	"github.com/gorilla/websocket"
)

func sleepCtx(ctx context.Context, duration time.Duration) {
	select {
	case <-time.After(duration):
	case <-ctx.Done():
	}
}

// Make a webscoket connection. Retries the connection with exponential backoff if the
// connection attempt fails.
func connect(ctx context.Context, url string, enableCompression bool) (*websocket.Conn, error) {
	sleep := time.Second
	var err error
	var conn *websocket.Conn
	for i := 0; i < 10; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			dialer := websocket.DefaultDialer
			dialer.EnableCompression = enableCompression
			conn, _, err = dialer.DialContext(ctx, url, nil)
			if err == nil {
				return conn, nil
			}
			// log.Printf("connection attempt %d failed: %v. Retrying in %s...", i+1, err, sleep)
			sleepCtx(ctx, sleep)
			sleep *= 2
		}
	}
	return nil, fmt.Errorf("failed to connect after retries: %w", err)
}

// Message is the data produced by the websocket.
type Message struct {
	buf  *bytes.Buffer
	pool *bufferPool
}

// Data returns the data stored in a Message. The byte slice should not be used after
// Release is called on a message.
func (m Message) Data() []byte {
	return m.buf.Bytes()
}

// Release the message back to the WebSocket's buffer pool. You should call this method
// after you are finished reading the message's data. Once called, the message's data
// is no longer valid, and should not be used.
func (m Message) Release() {
	m.pool.put(m.buf)
}

// type Options struct {
// 	EnableCompression bool

// 	PingInterval time.Duration

// 	ResetInterval time.Duration

// 	// PoolSize defines the size of the pool of pre-allocated buffers for storing data
// 	// received from the websocket connection. If not set, it will be set to 32 by
// 	// default.
// 	PoolSize int
// 	// BufCapacity sets the initial capacity of buffers in the websocket's buffer pool.
// 	// You should set this to be larger than the typical size of a message from the
// 	// specific websocket connection to reduce the number of memory allocations. If not
// 	// set, it will be set to 2048 bytes by default.
// 	BufCapacity int
// }

var defaultOptions tradekit.StreamOptions = tradekit.StreamOptions{
	EnableCompression: false,
	PingInterval:      15 * time.Second,
	ResetInterval:     time.Duration(1<<63 - 1),
	BufferPoolSize:    32,
	BufferCapacity:    2048,
}

func setOptionDefaults(opts *tradekit.StreamOptions) tradekit.StreamOptions {
	if opts == nil {
		return defaultOptions
	}
	res := *opts
	if res.PingInterval == 0 {
		res.PingInterval = defaultOptions.PingInterval
	}
	if res.ResetInterval == 0 {
		res.ResetInterval = defaultOptions.ResetInterval
	}
	if res.BufferCapacity == 0 {
		res.BufferCapacity = defaultOptions.BufferCapacity
	}
	if res.BufferPoolSize == 0 {
		res.BufferPoolSize = defaultOptions.BufferPoolSize
	}
	return res
}

// Websocket handles a websocket client connection to a given URL.
type Websocket struct {
	Url       string
	bufPool   *bufferPool
	responses chan Message
	requests  chan []byte
	close     chan struct{}
	errc      chan error
	closed    atomic.Bool
	wg        sync.WaitGroup
	OnConnect func() error
	opts      tradekit.StreamOptions
}

func New(url string, opts *tradekit.StreamOptions) Websocket {
	wsOpts := setOptionDefaults(opts)

	return Websocket{
		Url:       url,
		bufPool:   newBufferPool(wsOpts.BufferPoolSize, wsOpts.BufferCapacity),
		responses: make(chan Message, 10),
		requests:  make(chan []byte, 10),
		close:     make(chan struct{}, 1),
		errc:      make(chan error, 1),
		OnConnect: func() error { return nil },
		opts:      wsOpts,
	}
}

func (ws *Websocket) run(ctx context.Context, errc chan error, done chan<- struct{}) {
	defer func() { done <- struct{}{} }()

	conn, err := connect(ctx, ws.Url, ws.opts.EnableCompression)
	if err != nil {
		errc <- err
		return
	}
	if err := ws.OnConnect(); err != nil {
		errc <- err
		return
	}

	var stop atomic.Bool
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer func() {
			conn.Close()
			wg.Done()
		}()
		for {
			if stop.Load() {
				return
			}
			messageType, r, err := conn.NextReader()
			if err != nil {
				errc <- err
				return
			}
			if messageType == websocket.PingMessage {
				if err := conn.WriteMessage(websocket.PongMessage, nil); err != nil {
					errc <- err
					return
				}
			} else if messageType == websocket.TextMessage {
				buf := ws.bufPool.get()
				_, err = io.Copy(buf, r)
				if err != nil {
					ws.bufPool.put(buf)
					errc <- err
					return
				}
				msg := Message{buf: buf, pool: ws.bufPool}
				ws.responses <- msg

			}
		}
	}()

	wg.Add(1)
	go func() {
		pingTicker := time.NewTicker(ws.opts.PingInterval)
		defer func() {
			pingTicker.Stop()
			wg.Done()
		}()
		for {
			select {
			case <-ctx.Done():
				stop.Store(true)
				return
			case <-pingTicker.C:
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					stop.Store(true)
					ws.errc <- err
					return
				}
			case msg := <-ws.requests:
				if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
					stop.Store(true)
					ws.errc <- err
					return
				}
			}
		}

	}()

	wg.Wait()
}

// Start the websocket connection. This function creates the websocket connection and
// immediately begins reading messages sent by the server. Start must be called before
// any messages can be received by the consumer.
func (ws *Websocket) Start(ctx context.Context) error {
	restartCtx, cancel := context.WithCancel(ctx)
	errc := make(chan error, 1)
	done := make(chan struct{}, 1)
	go ws.run(restartCtx, errc, done)

	// Reconnect on any of these close codes
	reconnectOn := []int{
		websocket.CloseNormalClosure,
		websocket.CloseServiceRestart,
		websocket.CloseTryAgainLater,
		websocket.CloseAbnormalClosure,
		websocket.CloseNoStatusReceived,
	}

	go func() {
		resetTicker := time.NewTicker(ws.opts.ResetInterval)
		defer func() {
			ws.closed.Store(true)
			cancel()
			<-done
			close(ws.responses)
			close(ws.requests)
			close(ws.errc)
			close(ws.close)
			resetTicker.Stop()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-resetTicker.C:
				cancel()
				<-done
				restartCtx, cancel = context.WithCancel(ctx)
				go ws.run(restartCtx, errc, done)
			case err := <-errc:
				if websocket.IsCloseError(err, reconnectOn...) {
					cancel()
					<-done
					restartCtx, cancel = context.WithCancel(ctx)
					go ws.run(restartCtx, errc, done)
				} else {
					ws.errc <- err
					return
				}
			case <-ws.close:
				return
			}
		}
	}()

	return nil
}

// Send a text message along the websocket.
func (ws *Websocket) Send(data []byte) {
	ws.requests <- data
}

// Messages returns a channel containing the messages received from the websocket. Each
// Message received should be released back to the websocket's buffer pool by calling
// Release once you are finished with the message.
func (ws *Websocket) Messages() <-chan Message {
	return ws.responses
}

// Close sends a closes the websocket connection. The Messages channel will be closed
// immediately after.
func (ws *Websocket) Close() {
	if ws.closed.Load() {
		return
	}
	ws.close <- struct{}{}
}

func (ws *Websocket) Err() <-chan error {
	return ws.errc
}
