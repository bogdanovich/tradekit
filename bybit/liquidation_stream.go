package bybit

import (
	"fmt"

	"github.com/bogdanovich/tradekit/lib/tk"
)

// LiquidationSub represents a subscription to a liquidation stream. See [NewLiquidationStream]
type LiquidationSub struct {
	Symbol string
}

func (s LiquidationSub) channel() string {
	return fmt.Sprintf("liquidation.%s", s.Symbol)
}

// NewLiquidationStream returns a stream of liquidations. For details see:
//   - https://bybit-exchange.github.io/docs/v5/websocket/public/liquidation
func NewLiquidationStream(wsUrl string, subs []LiquidationSub, paramFuncs ...tk.Param) Stream[Liquidation] {
	subscriptions := make([]subscription, len(subs))
	for i, sub := range subs {
		subscriptions[i] = sub
	}
	params := streamParams[Liquidation]{
		name:         "LiquidationStream",
		wsUrl:        wsUrl,
		parseMessage: ParseLiquidation,
		subs:         subscriptions,
		Params:       tk.ApplyParams(paramFuncs),
	}
	return newStream[Liquidation](params)
}
