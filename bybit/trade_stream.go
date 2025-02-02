package bybit

import (
	"fmt"

	"github.com/bogdanovich/tradekit/lib/tk"
)

// OrderbookSub represents a subscription to the stream of trades for trading symbol.
// See [NewTradesStream].
type TradesSub struct {
	Symbol string
}

func (s TradesSub) channel() string {
	return fmt.Sprintf("publicTrade.%s", s.Symbol)
}

// NewTradesStream returns a stream of trades. For details see:
//   - https://bybit-exchange.github.io/docs/v5/websocket/public/trade
func NewTradesStream(wsUrl string, subs []TradesSub, paramFuncs ...tk.Param) Stream[Trades] {
	subscriptions := make([]subscription, len(subs))
	for i, sub := range subs {
		subscriptions[i] = sub
	}
	params := streamParams[Trades]{
		name:         "TradesStream",
		wsUrl:        wsUrl,
		parseMessage: ParseTradesMessage,
		subs:         subscriptions,
		Params:       tk.ApplyParams(paramFuncs),
	}
	return newStream[Trades](params)
}
