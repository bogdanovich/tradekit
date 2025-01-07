package bybit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bogdanovich/tradekit/internal/set"
	"github.com/bogdanovich/tradekit/internal/websocket"
	"github.com/bogdanovich/tradekit/lib/tk"
	"github.com/valyala/fastjson"
)

// replicate the reconnectable errors from deribit
var reconnectableErrors = []string{
	"read tcp",
	"write tcp",
	"websocket: close sent",
	"websocket: bad handshake",
}

type subscription interface {
	channel() string
}

// streamParams holds creation parameters for a stream, similar to the Deribit approach.
type streamParams[T any] struct {
	name         string
	wsUrl        string
	parseMessage func(*fastjson.Value) T
	subs         []subscription
	*tk.Params
}

// Stream is our public interface.
type Stream[T any] interface {
	Start(context.Context) error
	Messages() <-chan T
	Err() <-chan error
	PendingMessagesCount() int
}

// stream implements Stream[T]. It uses tk.Params for logger, credentials, buffer size, etc.
type stream[T any] struct {
	name              string
	url               string
	msgs              chan T
	errc              chan error
	parseMessage      func(*fastjson.Value) T
	subscriptions     set.Set[string]
	subscribeAllReq   chan struct{}
	p                 fastjson.Parser
	closed            atomic.Bool
	logger            tk.Logger
	creds             *tk.Credentials
	channelBufferSize int
}

// newStream constructs a new stream using the provided params and optional subscriptions.
func newStream[T any](p streamParams[T]) *stream[T] {
	// If no params passed, use defaults
	if p.Params == nil {
		p.Params = tk.DefaultParams()
	}

	// Build the set of subscribed channels
	subscriptions := set.New[string]()
	for _, s := range p.subs {
		subscriptions.Add(s.channel())
	}

	return &stream[T]{
		name:              p.name,
		url:               p.wsUrl,
		parseMessage:      p.parseMessage,
		subscriptions:     subscriptions,
		subscribeAllReq:   make(chan struct{}, 1),
		logger:            p.Params.Logger,
		creds:             p.Params.Credentials,
		channelBufferSize: p.Params.ChannelBufferSize,
		// We'll create buffered channels using the user-specified size
		msgs: make(chan T, p.Params.ChannelBufferSize),
		errc: make(chan error, 1),
	}
}

// Messages returns a channel of streamed items (T).
func (s *stream[T]) Messages() <-chan T {
	return s.msgs
}

// Err returns a channel which will receive errors that cause the stream to exit.
func (s *stream[T]) Err() <-chan error {
	return s.errc
}

func (s *stream[T]) PendingMessagesCount() int {
	return len(s.msgs)
}

// Start spins up the main loop with reconnect logic.
func (s *stream[T]) Start(ctx context.Context) error {
	restartChan := make(chan struct{}, 1)

	go func() {
		defer func() {
			s.closed.Store(true)
			close(s.msgs)
			close(s.errc)
			close(s.subscribeAllReq)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case <-restartChan:
				// Similar to deribit, wait 5s before reconnect
				s.logger.Info(fmt.Sprintf("Bybit %s: reconnecting in 5 seconds...", s.name))
				time.Sleep(5 * time.Second)

				if err := s.startWebsocketStream(ctx, restartChan); err != nil {
					select {
					case s.errc <- s.nameErr(err):
					default:
					}
					return
				}
				s.logger.Info(fmt.Sprintf("Bybit %s: reconnected", s.name))
			}
		}
	}()

	// Initial connection
	return s.startWebsocketStream(ctx, restartChan)
}

// startWebsocketStream dials the websocket, sets up read loops, listens for errors.
func (s *stream[T]) startWebsocketStream(ctx context.Context, restartChan chan struct{}) error {
	s.closed.Store(false)

	ws := websocket.New(s.url, nil)
	if s.logger == nil {
		s.logger = &tk.NoOpLogger{}
	}

	ws.OnConnect = func() error {
		// Once connected, we trigger a full re-subscribe
		s.subscribeAllReq <- struct{}{}
		return nil
	}

	if err := ws.Start(ctx); err != nil {
		return s.nameErr(fmt.Errorf("connecting to websocket: %w", err))
	}

	// Goroutine to read messages, errors, and handle heartbeats.
	go func() {
		defer ws.Close()

		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case msg := <-ws.Messages():
				if err := s.handleMessage(msg); err != nil {
					s.errc <- s.nameErr(err)
					return
				}

			case <-ticker.C:
				ws.Send(heartbeatMsg())

			case <-s.subscribeAllReq:
				// re-subscribe to all channels
				if err := s.subscribeAll(&ws); err != nil {
					s.errc <- s.nameErr(err)
					return
				}

			case err := <-ws.Err():
				// check for reconnect
				if s.shouldReconnect(err) {
					s.logger.Error(fmt.Sprintf("Bybit %s: connection error: %v", s.name, err))
					select {
					case restartChan <- struct{}{}:
					default:
					}
					return
				}
				s.errc <- s.nameErr(err)
				return
			}
		}
	}()

	return nil
}

// shouldReconnect checks if error string matches any known reconnectable pattern.
func (s *stream[T]) shouldReconnect(err error) bool {
	if err == nil {
		return false
	}
	for _, reconnectable := range reconnectableErrors {
		if strings.Contains(err.Error(), reconnectable) {
			return true
		}
	}
	return false
}

// handleMessage parses raw JSON, calls parseMessage, pushes to msgs channel.
func (s *stream[T]) handleMessage(msg websocket.Message) error {
	defer msg.Release()

	v, err := s.p.ParseBytes(msg.Data())
	if err != nil {
		return err
	}

	op := v.GetStringBytes("op")
	if equalAny(op, "pong", "ping", "subscribe", "unsubscribe") {
		return nil
	}

	m := s.parseMessage(v)
	s.msgs <- m
	return nil
}

// subscribeAll uses the current subscriptions list to send an op=subscribe message.
func (s *stream[T]) subscribeAll(ws *websocket.Websocket) error {
	if s.closed.Load() {
		return fmt.Errorf("stream is closed")
	}
	msg, err := subscribeMsg(s.subscriptions.Slice())
	if err != nil {
		return err
	}
	ws.Send(msg)
	return nil
}

func subscribeMsg(channels []string) ([]byte, error) {
	msg := map[string]interface{}{
		"op":   "subscribe",
		"args": channels,
	}
	return json.Marshal(msg)
}

func (s *stream[T]) nameErr(err error) error {
	return fmt.Errorf("Bybit %s: %w", s.name, err)
}

// simple helper to check if op is in any of the provided strings
func equalAny(b []byte, strs ...string) bool {
	for _, s := range strs {
		if bytes.Equal(b, []byte(s)) {
			return true
		}
	}
	return false
}

// heartbeatMsg is the periodic ping we send.
func heartbeatMsg() []byte {
	return []byte(`{"op": "ping"}`)
}
