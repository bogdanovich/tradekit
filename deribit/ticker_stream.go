package deribit

import (
	"fmt"

	"github.com/valyala/fastjson"
)

// TickerSub represents a subscription to a Deribit ticker stream created using [NewTickerStream].
// For details see: https://docs.deribit.com/#ticker-instrument_name-interval
type TickerSub struct {
	Instrument string
	Interval   string
}

type Ticker struct {
	Timestamp              int64         `json:"timestamp"`
	Instrument             string        `json:"instrument_name"`
	BestAskPrice           float64       `json:"best_ask_price"`
	BestAskAmount          float64       `json:"best_ask_amount"`
	BestBidPrice           float64       `json:"best_bid_price"`
	BestBidAmount          float64       `json:"best_bid_amount"`
	LastPrice              float64       `json:"last_price"`
	MarkPrice              float64       `json:"mark_price"`
	IndexPrice             float64       `json:"index_price"`
	OpenInterest           float64       `json:"open_interest"`
	State                  string        `json:"state"`
	Stats                  TickerStats   `json:"stats"`
	Greeks                 *TickerGreeks `json:"greeks,omitempty"`
	EstimatedDeliveryPrice float64       `json:"estimated_delivery_price"`
	UnderlyingPrice        *float64      `json:"underlying_price,omitempty"`
}

type TickerStats struct {
	High        float64 `json:"high"`
	Low         float64 `json:"low"`
	PriceChange float64 `json:"price_change"`
	Volume      float64 `json:"volume"`
	VolumeUSD   float64 `json:"volume_usd,omitempty"`
}

type TickerGreeks struct {
	Delta float64 `json:"delta"`
	Gamma float64 `json:"gamma"`
	Theta float64 `json:"theta"`
	Vega  float64 `json:"vega"`
	Rho   float64 `json:"rho"`
}

func (sub TickerSub) channel() string {
	return fmt.Sprintf("ticker.%s.%s", sub.Instrument, sub.Interval)
}

// NewTickerStream creates a new [Stream] which produces a stream of ticker updates.
// For details see: https://docs.deribit.com/#ticker-instrument_name-interval
func NewTickerStream(wsUrl string, subscriptions ...TickerSub) Stream[Ticker, TickerSub] {
	p := streamParams[Ticker, TickerSub]{
		name:         "TickerStream",
		wsUrl:        wsUrl,
		isPrivate:    false,
		parseMessage: parseTicker,
		subs:         subscriptions,
	}
	s := newStream[Ticker](p)
	return s
}

func parseTicker(v *fastjson.Value) Ticker {
	return Ticker{
		Timestamp:              v.GetInt64("timestamp"),
		Instrument:             string(v.GetStringBytes("instrument_name")),
		BestAskPrice:           v.GetFloat64("best_ask_price"),
		BestAskAmount:          v.GetFloat64("best_ask_amount"),
		BestBidPrice:           v.GetFloat64("best_bid_price"),
		BestBidAmount:          v.GetFloat64("best_bid_amount"),
		LastPrice:              v.GetFloat64("last_price"),
		MarkPrice:              v.GetFloat64("mark_price"),
		IndexPrice:             v.GetFloat64("index_price"),
		OpenInterest:           v.GetFloat64("open_interest"),
		State:                  string(v.GetStringBytes("state")),
		Stats:                  parseTickerStats(v.Get("stats")),
		Greeks:                 parseTickerGreeks(v.Get("greeks")),
		EstimatedDeliveryPrice: v.GetFloat64("estimated_delivery_price"),
		UnderlyingPrice:        parseOptionalFloat(v, "underlying_price"),
	}
}

func parseTickerStats(v *fastjson.Value) TickerStats {
	if v == nil {
		return TickerStats{}
	}
	return TickerStats{
		High:        v.GetFloat64("high"),
		Low:         v.GetFloat64("low"),
		PriceChange: v.GetFloat64("price_change"),
		Volume:      v.GetFloat64("volume"),
		VolumeUSD:   v.GetFloat64("volume_usd"),
	}
}

func parseTickerGreeks(v *fastjson.Value) *TickerGreeks {
	if v == nil {
		return nil
	}
	return &TickerGreeks{
		Delta: v.GetFloat64("delta"),
		Gamma: v.GetFloat64("gamma"),
		Theta: v.GetFloat64("theta"),
		Vega:  v.GetFloat64("vega"),
		Rho:   v.GetFloat64("rho"),
	}
}

func parseOptionalFloat(v *fastjson.Value, key string) *float64 {
	if value := v.Get(key); value != nil {
		f := value.GetFloat64()
		return &f
	}
	return nil
}
