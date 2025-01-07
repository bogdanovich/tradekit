package bybit

import (
	"fmt"

	"github.com/bogdanovich/tradekit/lib/tk"
)

// OrderbookSub represents a subscription to the orderbook stream of trading symbol.
// See [NewOrderbookStream].
type OrderbookSub struct {
	Symbol string
	Depth  int
}

func (s OrderbookSub) channel() string {
	return fmt.Sprintf("orderbook.%d.%s", s.Depth, s.Symbol)
}

// NewLiquidationStream returns a stream of orderbook updates. For details see:
//   - https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook
func NewOrderbookStream(wsUrl string, subs []OrderbookSub, paramFuncs ...tk.Param) Stream[OrderbookUpdateMessage] {
	subscriptions := make([]subscription, len(subs))
	for i, sub := range subs {
		subscriptions[i] = sub
	}
	params := streamParams[OrderbookUpdateMessage]{
		name:         "OrderbookStream",
		wsUrl:        wsUrl,
		parseMessage: ParseOrderbookUpdateMessage,
		subs:         subscriptions,
		Params:       tk.ApplyParams(paramFuncs),
	}
	return newStream[OrderbookUpdateMessage](params)
}
