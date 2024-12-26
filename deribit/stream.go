package deribit

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bogdanovich/tradekit"
	"github.com/bogdanovich/tradekit/internal/set"
	"github.com/bogdanovich/tradekit/internal/websocket"
	"github.com/bogdanovich/tradekit/lib/tk"
	"github.com/valyala/fastjson"
)

// A Stream represents a connection to a collection of Deribit streaming subscriptions
// over a websocket connection. For more details see:
//   - https://docs.deribit.com/#subscriptions
//
// The following functions create Streams to public channels:
//   - [NewTradesStream]
//   - [NewOrderbookStream]
//   - [NewOrderbookDepthStream]
//   - [NewInstrumentStateStream]
//
// The following functions create Streams to private user channels:
//   - [NewUserTradesStream]
//   - [NewUserOrdersStream]
type Stream[T any, U subscription] interface {
	// SetStreamOptions sets optional parameters for the stream. If used, it should be
	// called before Start.
	SetStreamOptions(*tradekit.StreamOptions)

	// SetCredentials sets credentials for authentication with Deribit. Check the Deribit
	// documentation to see if authentication is required for a particular stream.
	SetCredentials(*tk.Credentials)

	// Start the stream. The stream must be started before any messages will be received
	// or any new subscriptions may be made.
	Start(context.Context) error

	// Messages returns a channel of messages received from the stream's subscriptions.
	Messages() <-chan T

	// Err returns a channel which produces an error when there is an irrevocable failure
	// with the stream's connection. It should be read concurrently with the Messages
	// channel. If the channel produces and error, the stream stops and the Messages
	// channel is closed, and no further subscriptions may be made.
	Err() <-chan error

	// Subscribe adds a new subscription to the stream. This is a no-op if the
	// subscription already exists.
	Subscribe(subs ...U)

	// Unsubscribe removes a subscription from the stream. This is a no-op if the
	// subscription does not already exist.
	Unsubscribe(subs ...U)

	// PendingMessagesCount returns the number of messages that have been received but not
	// read from the Messages channel.
	PendingMessagesCount() int
}

// stream makes subscriptions to Deribit channels and provides a channel to receive
// the messages they produce.
type stream[T any, U subscription] struct {
	name          string
	url           string
	msgs          chan T
	errc          chan error
	parseMessage  func(*fastjson.Value) T
	isPrivate     bool
	subscriptions set.Set[string]
	opts          *tradekit.StreamOptions
	credentials   *tk.Credentials

	subRequests          chan []U
	unsubRequests        chan []U
	subscribeAllRequests chan struct{}

	closed atomic.Bool
	p      fastjson.Parser
	*tk.Params
}

type subscription interface {
	channel() string
}

type streamParams[T any, U subscription] struct {
	name         string
	wsUrl        string
	isPrivate    bool
	parseMessage func(*fastjson.Value) T
	subs         []U
	*tk.Params
}

var reconnectableErrors = []string{
	"read tcp",
	"write tcp",
	"websocket: close sent",
	"websocket: bad handshake",
}

func newStream[T any, U subscription](p streamParams[T, U]) *stream[T, U] {

	channels := make([]string, len(p.subs))
	for i, sub := range p.subs {
		channels[i] = sub.channel()
	}
	if p.Params == nil {
		p.Params = tk.DefaultParams()
	}

	return &stream[T, U]{
		name:                 p.name,
		url:                  p.wsUrl,
		msgs:                 make(chan T, p.ChannelBufferSize),
		errc:                 make(chan error, 1),
		parseMessage:         p.parseMessage,
		subscriptions:        set.New[string](channels...),
		isPrivate:            p.isPrivate,
		subRequests:          make(chan []U, 10),
		unsubRequests:        make(chan []U, 10),
		subscribeAllRequests: make(chan struct{}, 10),
		Params:               p.Params,
	}
}

func (s *stream[T, U]) SetStreamOptions(opts *tradekit.StreamOptions) {
	s.opts = opts
}

func (s *stream[T, U]) SetCredentials(c *tk.Credentials) {
	s.Params.Credentials = c
}

func (s *stream[T, U]) Start(ctx context.Context) error {
	// Create a channel to signal stream restart
	restartChan := make(chan struct{}, 1)

	go func() {
		defer func() {
			s.closed.Store(true)
			close(s.msgs)
			close(s.errc)
			close(s.subRequests)
			close(s.unsubRequests)
			close(s.subscribeAllRequests)
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-restartChan:
				s.Logger.Info(s.namePrefix("reconnecting in 5 seconds..."))
				time.Sleep(5 * time.Second)

				// Perform the websocket connection and stream setup
				if err := s.startWebsocketStream(ctx, restartChan); err != nil {
					// If startup fails, send the error and potentially stop
					select {
					case s.errc <- s.nameErr(err):
					default:
					}
					return
				}
				s.Logger.Info(s.namePrefix("reconnected"))
			}
		}
	}()

	// Initial stream start
	return s.startWebsocketStream(ctx, restartChan)
}

func (s *stream[T, U]) startWebsocketStream(
	ctx context.Context,
	restartChan chan struct{},
) error {
	// Reset closed state
	s.closed.Store(false)

	ws := websocket.New(s.url, s.opts)

	ws.OnConnect = func() error {
		if s.Params.Credentials != nil {
			if s.closed.Load() {
				return nil
			}
			// we don't need to wait for response, just send the auth request
			// since handleMessage skips all incoming messages without a method field
			_, err := s.sendAuthRequest(&ws)
			if err != nil {
				return fmt.Errorf("auth failure: %w", err)
			}
		}

		s.subscribeAllRequests <- struct{}{}
		return nil
	}

	go func() {
		defer func() {
			ws.Close()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-ws.Messages():
				if err := s.handleMessage(msg); err != nil {
					s.errc <- s.nameErr(err)
					return
				}
			case subs := <-s.subRequests:
				if err := s.subscribe(&ws, subs...); err != nil {
					s.errc <- s.nameErr(err)
					return
				}
			case subs := <-s.unsubRequests:
				if err := s.unsubscribe(&ws, subs...); err != nil {
					s.errc <- s.nameErr(err)
					return
				}
			case <-s.subscribeAllRequests:
				if err := s.subscribeAll(&ws); err != nil {
					s.errc <- s.nameErr(err)
					return
				}
			case err := <-ws.Err():
				// Check if the error requires a reconnection attempt
				if s.shouldReconnect(err) {
					// Signal for restart, non-blocking
					s.Logger.Error(s.nameErr(err).Error())
					select {
					case restartChan <- struct{}{}:
					default:
					}
					return
				}

				// Only send non-reconnectable errors to the error channel
				s.errc <- s.nameErr(err)
				return
			}
		}
	}()

	if err := ws.Start(ctx); err != nil {
		return s.nameErr(fmt.Errorf("connecting to websocket: %w", err))
	}

	return nil
}

func (s *stream[T, U]) shouldReconnect(err error) bool {
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

func (s *stream[T, U]) handleMessage(msg websocket.Message) error {
	defer msg.Release()

	v, err := s.p.ParseBytes(msg.Data())
	if err != nil {
		return fmt.Errorf("invalid message: %s", string(msg.Data()))
	}

	method := v.GetStringBytes("method")
	if method == nil {
		// This might be a subscribe/unsubscribe response or some other non-method message.
		if v.Get("error") != nil {
			return fmt.Errorf("received error response: %s", string(msg.Data()))
		}
		// If it's a known subscription response, you can safely return, otherwise log:
		return nil
	}

	params := v.Get("params")
	if params == nil {
		return fmt.Errorf(`field "params" is missing: %s`, string(msg.Data()))
	}

	channel := string(params.GetStringBytes("channel"))
	if !s.subscriptions.Exists(channel) {
		// We've received a message from a channel which we're no longer subscribed to. It's
		// okay to ignore it.
		return nil
	}

	data := params.Get("data")
	if data == nil {
		return fmt.Errorf(`field "params.data" is missing: %s`, string(msg.Data()))
	}
	s.msgs <- s.parseMessage(data)
	return nil
}

// Subscribe to all of the stream's subscriptions
func (s *stream[T, U]) subscribeAll(ws *websocket.Websocket) error {
	if s.closed.Load() {
		return errors.New("stream is closed")
	}
	if s.subscriptions.Len() == 0 {
		return nil
	}

	var method rpcMethod
	if s.isPrivate {
		method = methodPrivateSubscribe
	} else {
		method = methodPublicSubscribe
	}

	// make the subscriptions in chunks of maxsize 20. Otherwise, the message may be
	// too large for the server and it will close the connection.
	for _, channels := range chunk(s.subscriptions.Slice(), 20) {
		id := genId()
		params := map[string]interface{}{"channels": channels}
		msg, err := rpcRequestMsg(method, id, params)
		if err != nil {
			return err
		}
		ws.Send(msg)
	}
	return nil
}

func (s *stream[T, U]) Subscribe(subs ...U) {
	if s.closed.Load() {
		return
	}
	s.subRequests <- subs
}

func (s *stream[T, U]) Unsubscribe(subs ...U) {
	if s.closed.Load() {
		return
	}
	s.unsubRequests <- subs
}

// subscribe to the provided slice of channels. If a channel already exists in the
// stream's subscriptions then it will be ignored. Returns an error if the stream is
// closed.
func (s *stream[T, U]) subscribe(ws *websocket.Websocket, subs ...U) error {
	if s.closed.Load() {
		return errors.New("stream is closed")
	}
	newChannels := make([]string, 0)
	for _, sub := range subs {
		c := sub.channel()
		if !s.subscriptions.Exists(c) {
			newChannels = append(newChannels, c)
			s.subscriptions.Add(c)
		}
	}

	if len(newChannels) == 0 {
		return nil
	}

	var method rpcMethod
	if s.isPrivate {
		method = methodPrivateSubscribe
	} else {
		method = methodPublicSubscribe
	}

	// make the subscriptions in chunks of maxsize 20. Otherwise, the message may be
	// too large for the server and it will close the connection.
	for _, channels := range chunk(newChannels, 20) {
		id := genId()
		params := map[string]interface{}{"channels": channels}
		msg, err := rpcRequestMsg(method, id, params)
		if err != nil {
			return err
		}
		ws.Send(msg)

	}
	return nil
}

// unsubscribe from the provided slice of channels. Returns an error if the stream is
// closed.
func (s *stream[T, U]) unsubscribe(ws *websocket.Websocket, subs ...U) error {
	if s.closed.Load() {
		return errors.New("stream is closed")
	}
	removeChannels := make([]string, 0)
	for _, sub := range subs {
		c := sub.channel()
		if s.subscriptions.Pop(c) {
			removeChannels = append(removeChannels, c)
		}
	}

	if len(removeChannels) == 0 {
		return nil
	}

	var method rpcMethod
	if s.isPrivate {
		method = methodPrivateUnsubscribe
	} else {
		method = methodPublicUnsubscribe
	}

	// make the subscriptions in chunks of maxsize 20. Otherwise, the message may be
	// too large for the server and it will close the connection.
	for _, channels := range chunk(removeChannels, 20) {
		id := genId()
		params := map[string]interface{}{"channels": channels}
		msg, err := rpcRequestMsg(method, id, params)
		if err != nil {
			return err
		}
		ws.Send(msg)
	}

	return nil
}

func (s *stream[T, U]) Err() <-chan error {
	return s.errc
}

func (s *stream[T, U]) Messages() <-chan T {
	return s.msgs
}

func (s *stream[T, U]) PendingMessagesCount() int {
	return len(s.msgs)
}

func (s *stream[T, U]) nameErr(err error) error {
	return fmt.Errorf("deribit %s: %w", s.name, err)
}

func (s *stream[T, U]) namePrefix(msg string) string {
	return fmt.Sprintf("deribit %s: %s", s.name, msg)
}

// sendAuthRequest sends auth request along the stream's websocket
func (s *stream[T, U]) sendAuthRequest(ws *websocket.Websocket) (id int64, err error) {
	id = genId()
	method := methodPublicAuth
	params := map[string]string{
		"grant_type":    "client_credentials",
		"client_id":     s.Params.ClientId,
		"client_secret": s.Params.ClientSecret,
	}
	msg, err := rpcRequestMsg(method, id, params)
	if err != nil {
		return
	}
	ws.Send(msg)
	return
}

func genId() int64 {
	return time.Now().UnixNano()
}
