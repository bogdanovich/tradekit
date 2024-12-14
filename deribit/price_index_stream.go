package deribit

import (
	"fmt"

	"github.com/antibubblewrap/tradekit/lib/tk"
	"github.com/valyala/fastjson"
)

// PriceIndexSub represents a subscription to a Deribit price index created using [NewPriceIndexStream].
// For details see: https://docs.deribit.com/#deribit_price_index-index_name
type PriceIndexSub struct {
	IndexName string
}

type PriceIndex struct {
	Timestamp int64
	Price     float64
	IndexName string
}

func (sub PriceIndexSub) channel() string {
	return fmt.Sprintf("deribit_price_index.%s", sub.IndexName)
}

// NewPriceIndexStream creates a new [Stream] which produces a stream of price index updates.
// For details see: https://docs.deribit.com/#deribit_price_index-index_name
func NewPriceIndexStream(wsUrl string, subscriptions []PriceIndexSub, paramFuncs ...tk.Param) Stream[PriceIndex, PriceIndexSub] {

	p := streamParams[PriceIndex, PriceIndexSub]{
		name:         "PriceIndexStream",
		wsUrl:        wsUrl,
		isPrivate:    false,
		parseMessage: parsePriceIndex,
		subs:         subscriptions,
		Params:       tk.ApplyParams(paramFuncs),
	}
	s := newStream[PriceIndex](p)
	return s
}

func parsePriceIndex(v *fastjson.Value) PriceIndex {
	return PriceIndex{
		Timestamp: v.GetInt64("timestamp"),
		Price:     v.GetFloat64("price"),
		IndexName: string(v.GetStringBytes("index_name")),
	}
}
