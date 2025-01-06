package bybit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bogdanovich/tradekit"
	"github.com/bogdanovich/tradekit/internal/set"
	"github.com/bogdanovich/tradekit/internal/websocket"
	"github.com/valyala/fastjson"
)

// replicate the reconnectable errors from deribit
var reconnectableErrors = []string{
	"read tcp",
	"write tcp",
	"websocket: close sent",
	"websocket: bad handshake",
}

type Stream[T any] interface {
	SetOptions(*tradekit.StreamOptions)
	Start(context.Context) error
	Messages() <-chan T
	Err() <-chan error
}

type subscription interface {
	channel() string
}

func heartbeatMsg() []byte {
	return []byte(`{"op": "ping"}`)
}

func isPingOrSubscribeMsg(v *fastjson.Value) bool {
	op := v.GetStringBytes("op")
	if bytes.Equal(op, []byte("ping")) || bytes.Equal(op, []byte("subscribe")) {
		return true
	}
	return false
}

type stream[T any] struct {
	name string
	url  string

	msgs chan T
	errc chan error

	parseMessage  func(*fastjson.Value) (T, error)
	subscriptions set.Set[string]

	subscribeAllRequests chan struct{}

	opts   *tradekit.StreamOptions
	p      fastjson.Parser
	closed atomic.Bool // used to mark the stream closed
}

// newStream is unchanged, except we keep everything as is.
func newStream[T any](url string, name string, parseMessage func(*fastjson.Value) (T, error), subs []subscription) *stream[T] {
	subscriptions := set.New[string]()
	for _, s := range subs {
		subscriptions.Add(s.channel())
	}

	return &stream[T]{
		name:                 name,
		url:                  url,
		msgs:                 make(chan T, 10),
		errc:                 make(chan error, 1),
		parseMessage:         parseMessage,
		subscriptions:        subscriptions,
		subscribeAllRequests: make(chan struct{}, 1),
	}
}

func (s *stream[T]) SetOptions(opts *tradekit.StreamOptions) {
	s.opts = opts
}

// Start uses an approach similar to deribit, with a restartChan that signals
// when we need to reconnect.
func (s *stream[T]) Start(ctx context.Context) error {
	// A channel for restart signals
	restartChan := make(chan struct{}, 1)

	go func() {
		defer func() {
			s.closed.Store(true)
			close(s.msgs)
			close(s.errc)
			close(s.subscribeAllRequests)
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case <-restartChan:
				// Wait 5 seconds, just like we do in deribit’s code, then reconnect.
				time.Sleep(5 * time.Second)
				if err := s.startWebsocketStream(ctx, restartChan); err != nil {
					select {
					case s.errc <- s.nameErr(err):
					default:
					}
					return
				}
			}
		}
	}()

	// Perform the initial connection
	return s.startWebsocketStream(ctx, restartChan)
}

// startWebsocketStream tries to open the websocket, set OnConnect, then reads messages/errors.
// If a reconnectable error is encountered, it signals on restartChan.
func (s *stream[T]) startWebsocketStream(ctx context.Context, restartChan chan struct{}) error {
	s.closed.Store(false)

	ws := websocket.New(s.url, s.opts)
	ws.OnConnect = func() error {
		// Trigger subscribeAllRequests so all channels are re-subscribed
		s.subscribeAllRequests <- struct{}{}
		return nil
	}

	// Actually start the WS connection
	if err := ws.Start(ctx); err != nil {
		return s.nameErr(fmt.Errorf("connecting to websocket: %w", err))
	}

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

			// send heartbeats
			case <-ticker.C:
				ws.Send(heartbeatMsg())

			// re-subscribe upon request
			case <-s.subscribeAllRequests:
				if err := s.subscribeAll(&ws); err != nil {
					s.errc <- s.nameErr(err)
					return
				}

			case err := <-ws.Err():
				// If we get a reconnectable error, signal a restart
				if s.shouldReconnect(err) {
					select {
					case restartChan <- struct{}{}:
					default:
					}
					return
				}
				// Otherwise, pass it to s.errc and exit
				s.errc <- s.nameErr(err)
				return
			}
		}
	}()

	return nil
}

// shouldReconnect is exactly as in deribit’s code, checking if the error contains any reconnectable keyword.
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

func (s *stream[T]) Err() <-chan error {
	return s.errc
}

func (s *stream[T]) Messages() <-chan T {
	return s.msgs
}

// handleMessage is unchanged, except we do everything in place.
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

	m, err := s.parseMessage(v)
	if err != nil {
		return err
	}
	s.msgs <- m
	return nil
}

// subscribeAll re-subscribes to the entire subscriptions set.
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

func equalAny(b []byte, strs ...string) bool {
	for _, s := range strs {
		if bytes.Equal(b, []byte(s)) {
			return true
		}
	}
	return false
}
