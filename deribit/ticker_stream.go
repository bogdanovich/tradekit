package deribit

import (
	"fmt"

	"github.com/bogdanovich/tradekit/lib/tk"
	"github.com/valyala/fastjson"
)

// DeribitTickerSub represents a subscription to a Deribit ticker stream created using [NewTickerStream].
// For details see: https://docs.deribit.com/#ticker-instrument_name-interval
type DeribitTickerSub struct {
	Instrument string
	Interval   string
}

type DeribitTicker struct {
	Timestamp              int64         `json:"timestamp" parquet:"name=timestamp, type=INT64"`
	Instrument             string        `json:"instrument_name" parquet:"name=instrument, type=BYTE_ARRAY, convertedtype=UTF8"`
	BestAskPrice           float64       `json:"best_ask_price" parquet:"name=best_ask_price, type=DOUBLE"`
	BestAskAmount          float64       `json:"best_ask_amount" parquet:"name=best_ask_amount, type=DOUBLE"`
	BestBidPrice           float64       `json:"best_bid_price" parquet:"name=best_bid_price, type=DOUBLE"`
	BestBidAmount          float64       `json:"best_bid_amount" parquet:"name=best_bid_amount, type=DOUBLE"`
	LastPrice              float64       `json:"last_price" parquet:"name=last_price, type=DOUBLE"`
	MarkPrice              float64       `json:"mark_price" parquet:"name=mark_price, type=DOUBLE"`
	IndexPrice             float64       `json:"index_price" parquet:"name=index_price, type=DOUBLE"`
	OpenInterest           float64       `json:"open_interest" parquet:"name=open_interest, type=DOUBLE"`
	State                  string        `json:"state" parquet:"name=state, type=BYTE_ARRAY, convertedtype=UTF8"`
	Stats                  TickerStats   `json:"stats" parquet:"name=stats"`
	Greeks                 *TickerGreeks `json:"greeks,omitempty" parquet:"name=greeks"`
	EstimatedDeliveryPrice float64       `json:"estimated_delivery_price" parquet:"name=estimated_delivery_price, type=DOUBLE"`
	UnderlyingPrice        *float64      `json:"underlying_price,omitempty" parquet:"name=underlying_price, type=DOUBLE"`
	AskIV                  *float64      `json:"ask_iv,omitempty" parquet:"name=ask_iv, type=DOUBLE"`
	BidIV                  *float64      `json:"bid_iv,omitempty" parquet:"name=bid_iv, type=DOUBLE"`
	CurrentFunding         *float64      `json:"current_funding,omitempty" parquet:"name=current_funding, type=DOUBLE"`
	DeliveryPrice          *float64      `json:"delivery_price,omitempty" parquet:"name=delivery_price, type=DOUBLE"`
	Funding8h              *float64      `json:"funding_8h,omitempty" parquet:"name=funding_8h, type=DOUBLE"`
	InterestRate           *float64      `json:"interest_rate,omitempty" parquet:"name=interest_rate, type=DOUBLE"`
	InterestValue          *float64      `json:"interest_value,omitempty" parquet:"name=interest_value, type=DOUBLE"`
	MarkIV                 *float64      `json:"mark_iv,omitempty" parquet:"name=mark_iv, type=DOUBLE"`
	MaxPrice               *float64      `json:"max_price,omitempty" parquet:"name=max_price, type=DOUBLE"`
	MinPrice               *float64      `json:"min_price,omitempty" parquet:"name=min_price, type=DOUBLE"`
	SettlementPrice        *float64      `json:"settlement_price,omitempty" parquet:"name=settlement_price, type=DOUBLE"`
	UnderlyingIndex        string        `json:"underlying_index" parquet:"name=underlying_index, type=BYTE_ARRAY, convertedtype=UTF8"`
}

type TickerStats struct {
	High        float64 `json:"high" parquet:"name=high, type=DOUBLE"`
	Low         float64 `json:"low" parquet:"name=low, type=DOUBLE"`
	PriceChange float64 `json:"price_change" parquet:"name=price_change, type=DOUBLE"`
	Volume      float64 `json:"volume" parquet:"name=volume, type=DOUBLE"`
	VolumeUSD   float64 `json:"volume_usd,omitempty" parquet:"name=volume_usd, type=DOUBLE"`
}

type TickerGreeks struct {
	Delta float64 `json:"delta" parquet:"name=delta, type=DOUBLE"`
	Gamma float64 `json:"gamma" parquet:"name=gamma, type=DOUBLE"`
	Theta float64 `json:"theta" parquet:"name=theta, type=DOUBLE"`
	Vega  float64 `json:"vega" parquet:"name=vega, type=DOUBLE"`
	Rho   float64 `json:"rho" parquet:"name=rho, type=DOUBLE"`
}

func (sub DeribitTickerSub) channel() string {
	return fmt.Sprintf("ticker.%s.%s", sub.Instrument, sub.Interval)
}

// NewTickerStream creates a new [Stream] which produces a stream of ticker updates.
// For details see: https://docs.deribit.com/#ticker-instrument_name-interval
func NewTickerStream(wsUrl string, subscriptions []DeribitTickerSub, paramFuncs ...tk.Param) Stream[DeribitTicker, DeribitTickerSub] {
	p := streamParams[DeribitTicker, DeribitTickerSub]{
		name:         "TickerStream",
		wsUrl:        wsUrl,
		isPrivate:    false,
		parseMessage: parseTicker,
		subs:         subscriptions,
		Params:       tk.ApplyParams(paramFuncs),
	}
	s := newStream(p)
	return s
}

func parseTicker(v *fastjson.Value) DeribitTicker {
	return DeribitTicker{
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
		AskIV:                  parseOptionalFloat(v, "ask_iv"),
		BidIV:                  parseOptionalFloat(v, "bid_iv"),
		CurrentFunding:         parseOptionalFloat(v, "current_funding"),
		DeliveryPrice:          parseOptionalFloat(v, "delivery_price"),
		Funding8h:              parseOptionalFloat(v, "funding_8h"),
		InterestRate:           parseOptionalFloat(v, "interest_rate"),
		InterestValue:          parseOptionalFloat(v, "interest_value"),
		MarkIV:                 parseOptionalFloat(v, "mark_iv"),
		MaxPrice:               parseOptionalFloat(v, "max_price"),
		MinPrice:               parseOptionalFloat(v, "min_price"),
		SettlementPrice:        parseOptionalFloat(v, "settlement_price"),
		UnderlyingIndex:        string(v.GetStringBytes("underlying_index")),
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
