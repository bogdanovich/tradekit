package bybit

import (
	"fmt"

	"github.com/bogdanovich/tradekit/lib/tk"
)

// OrderbookSub represents a subscription to the orderbook stream of trading symbol.
// See [NewOrderbookStream].
type TickerSub struct {
	Symbol string
}

func (s TickerSub) channel() string {
	return fmt.Sprintf("tickers.%s", s.Symbol)
}

// NewLiquidationStream returns a stream of orderbook updates. For details see:
//   - https://bybit-exchange.github.io/docs/v5/websocket/public/ticker
//
// Spot & Option tickers message are snapshot only
func NewSpotTickerStream(wsUrl string, subs []TickerSub, paramFuncs ...tk.Param) Stream[SpotTickerMessage] {
	subscriptions := make([]subscription, len(subs))
	for i, sub := range subs {
		subscriptions[i] = sub
	}
	params := streamParams[SpotTickerMessage]{
		name:         "SpotTickerStream",
		wsUrl:        wsUrl,
		parseMessage: parseSpotTickerMessage,
		subs:         subscriptions,
		Params:       tk.ApplyParams(paramFuncs),
	}
	return newStream[SpotTickerMessage](params)
}

// NewFuturesTickerStream returns a stream of futures tickers. For details see:
//   - https://bybit-exchange.github.io/docs/v5/websocket/public/ticker
//
// TODO: Needs a solution to merge deltas and produce snapshots
func NewFuturesTickerStream(wsUrl string, subs []TickerSub, paramFuncs ...tk.Param) Stream[FuturesTickerMessage] {
	subscriptions := make([]subscription, len(subs))
	for i, sub := range subs {
		subscriptions[i] = sub
	}
	params := streamParams[FuturesTickerMessage]{
		name:         "FuturesTickerStream",
		wsUrl:        wsUrl,
		parseMessage: parseFuturesTickerMessage,
		subs:         subscriptions,
		Params:       tk.ApplyParams(paramFuncs),
	}
	return newStream[FuturesTickerMessage](params)
}
