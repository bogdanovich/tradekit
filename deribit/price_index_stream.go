package deribit

import (
	"fmt"

	"github.com/antibubblewrap/tradekit/lib/tk"
	"github.com/valyala/fastjson"
)

// DeribitPriceIndexSub represents a subscription to a Deribit price index created using [NewPriceIndexStream].
// For details see: https://docs.deribit.com/#deribit_price_index-index_name
type DeribitPriceIndexSub struct {
	IndexName string
}

type DeribitPriceIndex struct {
	Timestamp int64
	Price     float64
	IndexName string
}

func (sub DeribitPriceIndexSub) channel() string {
	return fmt.Sprintf("deribit_price_index.%s", sub.IndexName)
}

// NewPriceIndexStream creates a new [Stream] which produces a stream of price index updates.
// For details see: https://docs.deribit.com/#deribit_price_index-index_name
func NewPriceIndexStream(wsUrl string, subscriptions []DeribitPriceIndexSub, paramFuncs ...tk.Param) Stream[DeribitPriceIndex, DeribitPriceIndexSub] {

	p := streamParams[DeribitPriceIndex, DeribitPriceIndexSub]{
		name:         "price_index_stream",
		wsUrl:        wsUrl,
		isPrivate:    false,
		parseMessage: parsePriceIndex,
		subs:         subscriptions,
		Params:       tk.ApplyParams(paramFuncs),
	}
	s := newStream[DeribitPriceIndex](p)
	return s
}

func parsePriceIndex(v *fastjson.Value) DeribitPriceIndex {
	return DeribitPriceIndex{
		Timestamp: v.GetInt64("timestamp"),
		Price:     v.GetFloat64("price"),
		IndexName: string(v.GetStringBytes("index_name")),
	}
}
