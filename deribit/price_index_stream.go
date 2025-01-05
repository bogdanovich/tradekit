package deribit

import (
	"fmt"

	"github.com/bogdanovich/tradekit/lib/tk"
	"github.com/valyala/fastjson"
)

// DeribitPriceIndexSub represents a subscription to a Deribit price index created using [NewPriceIndexStream].
// For details see: https://docs.deribit.com/#deribit_price_index-index_name
type DeribitPriceIndexSub struct {
	IndexName string
}

type DeribitPriceIndex struct {
	Timestamp int64   `json:"timestamp" parquet:"name=timestamp, type=INT64"`
	Price     float64 `json:"price" parquet:"name=price, type=DOUBLE"`
	IndexName string  `json:"index_name" parquet:"name=index_name, type=BYTE_ARRAY, convertedtype=UTF8"`
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
		parseMessage: ParsePriceIndex,
		subs:         subscriptions,
		Params:       tk.ApplyParams(paramFuncs),
	}
	s := newStream[DeribitPriceIndex](p)
	return s
}

func ParsePriceIndex(v *fastjson.Value) DeribitPriceIndex {
	return DeribitPriceIndex{
		Timestamp: v.GetInt64("timestamp"),
		Price:     v.GetFloat64("price"),
		IndexName: string(v.GetStringBytes("index_name")),
	}
}
