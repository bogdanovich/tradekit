package deribit

import (
	"fmt"

	"github.com/bogdanovich/tradekit/lib/tk"
)

// OrderbookDepthSub represents a subscription to a deribit orderbook depth stream created
// using [NewOrderbookDepthStream]. The update interval is automatically set to 100ms.
// For details see:
//   - https://docs.deribit.com/#book-instrument_name-group-depth-interval
type OrderbookDepthSub struct {
	Instrument string
	Depth      int

	// Optional price grouping. A value of 0 means no grouping. Defaults to no grouping.
	Group int
}

func (sub OrderbookDepthSub) channel() string {
	if sub.Group == 0 {
		return fmt.Sprintf("book.%s.none.%d.100ms", sub.Instrument, sub.Depth)
	} else {
		return fmt.Sprintf("book.%s.%d.%d.100ms", sub.Instrument, sub.Group, sub.Depth)
	}
}

// NewOrderbookDepthStream creates a new [Stream] which produces a stream of orderbook
// depth snapshots. For a realtime stream of incremental orderbook updates, see
// [NewOrderbookStream]. For details see:
//   - https://docs.deribit.com/#book-instrument_name-group-depth-interval
func NewOrderbookDepthStream(wsUrl string, subscriptions []OrderbookDepthSub, paramFuncs ...tk.Param) Stream[OrderbookDepth, OrderbookDepthSub] {
	p := streamParams[OrderbookDepth, OrderbookDepthSub]{
		name:         "OrderbookDepthStream",
		wsUrl:        wsUrl,
		isPrivate:    false,
		parseMessage: ParseOrderbookDepth,
		subs:         subscriptions,
		Params:       tk.ApplyParams(paramFuncs),
	}
	s := newStream(p)
	return s
}
