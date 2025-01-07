package bybit

import (
	"fmt"
	"strconv"

	"github.com/bogdanovich/tradekit"
	"github.com/bogdanovich/tradekit/lib/conv"
	"github.com/valyala/fastjson"
)

type OrderbookUpdateMessage struct {
	Topic     string          `json:"topic" parquet:"name=topic, type=BYTE_ARRAY, convertedtype=UTF8"`
	Type      string          `json:"type" parquet:"name=type, type=BYTE_ARRAY, convertedtype=UTF8"`
	Timestamp int64           `json:"ts" parquet:"name=timestamp, type=INT64"`
	Data      OrderbookUpdate `json:"data" parquet:"name=data"`
}

type OrderbookUpdate struct {
	Symbol   string           `json:"s" parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8"`
	Bids     []tradekit.Level `json:"b" parquet:"name=bids, type=LIST"`
	Asks     []tradekit.Level `json:"a" parquet:"name=asks, type=LIST"`
	UpdateID int64            `json:"u" parquet:"name=update_id, type=INT64"`
	Sequence int64            `json:"seq" parquet:"name=sequence, type=INT64"`
}

type TradesMessage struct {
	Topic     string  `json:"topic" parquet:"name=topic, type=BYTE_ARRAY, convertedtype=UTF8"`
	Type      string  `json:"type" parquet:"name=type, type=BYTE_ARRAY, convertedtype=UTF8"`
	Timestamp int64   `json:"ts" parquet:"name=timestamp, type=INT64"`
	Data      []Trade `json:"data" parquet:"name=data, type=LIST"`
}

type Trade struct {
	Timestamp  int64   `json:"T" parquet:"name=timestamp, type=INT64"`
	Symbol     string  `json:"s" parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8"`
	Price      float64 `json:"p" parquet:"name=price, type=DOUBLE"`
	Amount     float64 `json:"v" parquet:"name=amount, type=DOUBLE"`
	Direction  string  `json:"S" parquet:"name=direction, type=BYTE_ARRAY, convertedtype=UTF8"`
	TradeID    string  `json:"i" parquet:"name=trade_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	BlockTrade bool    `json:"BT" parquet:"name=block_trade, type=BOOLEAN"`
}

type SpotTickerMessage struct {
	Topic         string     `json:"topic" parquet:"name=topic, type=BYTE_ARRAY, convertedtype=UTF8"` // Topic name
	Timestamp     int64      `json:"ts" parquet:"name=timestamp, type=INT64"`                         // Timestamp (ms) the system generates the data
	Type          string     `json:"type" parquet:"name=type, type=BYTE_ARRAY, convertedtype=UTF8"`   // Data type: snapshot
	CrossSequence int64      `json:"cs" parquet:"name=cross_sequence, type=INT64"`                    // Cross sequence
	Data          SpotTicker `json:"data" parquet:"name=data"`                                        // Ticker data
}

type SpotTicker struct {
	Symbol        string  `json:"symbol" parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8"` // Symbol name
	LastPrice     float64 `json:"lastPrice" parquet:"name=last_price, type=DOUBLE"`                  // Last price
	HighPrice24h  float64 `json:"highPrice24h" parquet:"name=high_price_24h, type=DOUBLE"`           // Highest price in the last 24 hours
	LowPrice24h   float64 `json:"lowPrice24h" parquet:"name=low_price_24h, type=DOUBLE"`             // Lowest price in the last 24 hours
	PrevPrice24h  float64 `json:"prevPrice24h" parquet:"name=prev_price_24h, type=DOUBLE"`           // Market price 24 hours ago
	Volume24h     float64 `json:"volume24h" parquet:"name=volume_24h, type=DOUBLE"`                  // Volume for 24 hours
	Turnover24h   float64 `json:"turnover24h" parquet:"name=turnover_24h, type=DOUBLE"`              // Turnover for 24 hours
	Price24hPcnt  float64 `json:"price24hPcnt" parquet:"name=price_24h_pcnt, type=DOUBLE"`           // Percentage change of market price in the last 24 hours
	USDIndexPrice float64 `json:"usdIndexPrice" parquet:"name=usd_index_price, type=DOUBLE"`         // USD index price (optional)
}

type FuturesTickerMessage struct {
	Topic         string        `json:"topic" parquet:"name=topic, type=BYTE_ARRAY, convertedtype=UTF8"` // Topic name
	Type          string        `json:"type" parquet:"name=type, type=BYTE_ARRAY, convertedtype=UTF8"`   // Data type: snapshot or delta
	CrossSequence int64         `json:"cs" parquet:"name=cross_sequence, type=INT64"`                    // Cross sequence
	Timestamp     int64         `json:"ts" parquet:"name=timestamp, type=INT64"`                         // Timestamp (ms) the system generates the data
	Data          FuturesTicker `json:"data" parquet:"name=data"`                                        // Ticker data
}

// needs a solution to handle optional fields (and produce snapshots from deltas)
type FuturesTicker struct {
	Symbol                 string  `json:"symbol" parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8"`                     // Symbol name
	TickDirection          string  `json:"tickDirection" parquet:"name=tick_direction, type=BYTE_ARRAY, convertedtype=UTF8"`      // Tick direction
	Price24hPcnt           float64 `json:"price24hPcnt" parquet:"name=price_24h_pcnt, type=DOUBLE"`                               // Percentage change in the last 24 hours
	LastPrice              float64 `json:"lastPrice" parquet:"name=last_price, type=DOUBLE"`                                      // Last price
	PrevPrice24h           float64 `json:"prevPrice24h" parquet:"name=prev_price_24h, type=DOUBLE"`                               // Market price 24 hours ago
	HighPrice24h           float64 `json:"highPrice24h" parquet:"name=high_price_24h, type=DOUBLE"`                               // Highest price in the last 24 hours
	LowPrice24h            float64 `json:"lowPrice24h" parquet:"name=low_price_24h, type=DOUBLE"`                                 // Lowest price in the last 24 hours
	PrevPrice1h            float64 `json:"prevPrice1h" parquet:"name=prev_price_1h, type=DOUBLE"`                                 // Market price an hour ago
	MarkPrice              float64 `json:"markPrice" parquet:"name=mark_price, type=DOUBLE"`                                      // Mark price
	IndexPrice             float64 `json:"indexPrice" parquet:"name=index_price, type=DOUBLE"`                                    // Index price
	OpenInterest           float64 `json:"openInterest" parquet:"name=open_interest, type=DOUBLE"`                                // Open interest size
	OpenInterestValue      float64 `json:"openInterestValue" parquet:"name=open_interest_value, type=DOUBLE"`                     // Open interest value
	Turnover24h            float64 `json:"turnover24h" parquet:"name=turnover_24h, type=DOUBLE"`                                  // Turnover for 24h
	Volume24h              float64 `json:"volume24h" parquet:"name=volume_24h, type=DOUBLE"`                                      // Volume for 24h
	NextFundingTime        int64   `json:"nextFundingTime" parquet:"name=next_funding_time, type=INT64"`                          // Next funding timestamp (ms)
	FundingRate            float64 `json:"fundingRate" parquet:"name=funding_rate, type=DOUBLE"`                                  // Funding rate
	Bid1Price              float64 `json:"bid1Price" parquet:"name=bid_1_price, type=DOUBLE"`                                     // Best bid price
	Bid1Size               float64 `json:"bid1Size" parquet:"name=bid_1_size, type=DOUBLE"`                                       // Best bid size
	Ask1Price              float64 `json:"ask1Price" parquet:"name=ask_1_price, type=DOUBLE"`                                     // Best ask price
	Ask1Size               float64 `json:"ask1Size" parquet:"name=ask_1_size, type=DOUBLE"`                                       // Best ask size
	DeliveryTime           int64   `json:"deliveryTime,omitempty" parquet:"name=delivery_time, type=INT64"`                       // Delivery date time (optional)
	BasisRate              float64 `json:"basisRate,omitempty" parquet:"name=basis_rate, type=DOUBLE"`                            // Basis rate (optional)
	DeliveryFeeRate        float64 `json:"deliveryFeeRate,omitempty" parquet:"name=delivery_fee_rate, type=DOUBLE"`               // Delivery fee rate (optional)
	PredictedDeliveryPrice float64 `json:"predictedDeliveryPrice,omitempty" parquet:"name=predicted_delivery_price, type=DOUBLE"` // Predicted delivery price (optional)
	PreOpenPrice           float64 `json:"preOpenPrice,omitempty" parquet:"name=pre_open_price, type=DOUBLE"`                     // Estimated pre-market open price (optional)
	PreQty                 float64 `json:"preQty,omitempty" parquet:"name=pre_qty, type=DOUBLE"`                                  // Estimated pre-market open qty (optional)
}

type Liquidation struct {
	Topic     string          `json:"topic"`
	Type      string          `json:"type"`
	Timestamp int64           `json:"ts"`
	Data      LiquidationData `json:"data"`
}

type LiquidationData struct {
	Price       float64 `json:"price"`
	Amount      float64 `json:"size"`
	Direction   string  `json:"side"`
	Symbol      string  `json:"symbol"`
	UpdatedTime int64   `json:"updatedTime"`
}

func parseFuturesTicker(v *fastjson.Value) (FuturesTicker, error) {
	return FuturesTicker{
		Symbol:                 string(v.GetStringBytes("symbol")),
		TickDirection:          string(v.GetStringBytes("tickDirection")),
		Price24hPcnt:           conv.BytesToFloat(v.GetStringBytes("price24hPcnt")),
		LastPrice:              conv.BytesToFloat(v.GetStringBytes("lastPrice")),
		PrevPrice24h:           conv.BytesToFloat(v.GetStringBytes("prevPrice24h")),
		HighPrice24h:           conv.BytesToFloat(v.GetStringBytes("highPrice24h")),
		LowPrice24h:            conv.BytesToFloat(v.GetStringBytes("lowPrice24h")),
		PrevPrice1h:            conv.BytesToFloat(v.GetStringBytes("prevPrice1h")),
		MarkPrice:              conv.BytesToFloat(v.GetStringBytes("markPrice")),
		IndexPrice:             conv.BytesToFloat(v.GetStringBytes("indexPrice")),
		OpenInterest:           conv.BytesToFloat(v.GetStringBytes("openInterest")),
		OpenInterestValue:      conv.BytesToFloat(v.GetStringBytes("openInterestValue")),
		Turnover24h:            conv.BytesToFloat(v.GetStringBytes("turnover24h")),
		Volume24h:              conv.BytesToFloat(v.GetStringBytes("volume24h")),
		NextFundingTime:        v.GetInt64("nextFundingTime"),
		FundingRate:            conv.BytesToFloat(v.GetStringBytes("fundingRate")),
		Bid1Price:              conv.BytesToFloat(v.GetStringBytes("bid1Price")),
		Bid1Size:               conv.BytesToFloat(v.GetStringBytes("bid1Size")),
		Ask1Price:              conv.BytesToFloat(v.GetStringBytes("ask1Price")),
		Ask1Size:               conv.BytesToFloat(v.GetStringBytes("ask1Size")),
		DeliveryTime:           v.GetInt64("deliveryTime"),
		BasisRate:              conv.BytesToFloat(v.GetStringBytes("basisRate")),
		DeliveryFeeRate:        conv.BytesToFloat(v.GetStringBytes("deliveryFeeRate")),
		PredictedDeliveryPrice: conv.BytesToFloat(v.GetStringBytes("predictedDeliveryPrice")),
		PreOpenPrice:           conv.BytesToFloat(v.GetStringBytes("preOpenPrice")),
		PreQty:                 conv.BytesToFloat(v.GetStringBytes("preQty")),
	}, nil
}

func parseFuturesTickerMessage(v *fastjson.Value) (FuturesTickerMessage, error) {
	data := v.Get("data")
	if data == nil {
		return FuturesTickerMessage{}, fmt.Errorf("missing field %q", "data")
	}
	futureTicker, err := parseFuturesTicker(data)
	if err != nil {
		return FuturesTickerMessage{}, err
	}
	return FuturesTickerMessage{
		Topic:         string(v.GetStringBytes("topic")),
		Timestamp:     v.GetInt64("ts"),
		Type:          string(v.GetStringBytes("type")),
		CrossSequence: v.GetInt64("cs"),
		Data:          futureTicker,
	}, nil
}

func parseSpotTicker(v *fastjson.Value) (SpotTicker, error) {
	return SpotTicker{
		Symbol:        string(v.GetStringBytes("symbol")),
		LastPrice:     conv.BytesToFloat(v.GetStringBytes("lastPrice")),
		HighPrice24h:  conv.BytesToFloat(v.GetStringBytes("highPrice24h")),
		LowPrice24h:   conv.BytesToFloat(v.GetStringBytes("lowPrice24h")),
		PrevPrice24h:  conv.BytesToFloat(v.GetStringBytes("prevPrice24h")),
		Volume24h:     conv.BytesToFloat(v.GetStringBytes("volume24h")),
		Turnover24h:   conv.BytesToFloat(v.GetStringBytes("turnover24h")),
		Price24hPcnt:  conv.BytesToFloat(v.GetStringBytes("price24hPcnt")),
		USDIndexPrice: conv.BytesToFloat(v.GetStringBytes("usdIndexPrice")),
	}, nil
}

func parseSpotTickerMessage(v *fastjson.Value) (SpotTickerMessage, error) {
	data := v.Get("data")
	if data == nil {
		return SpotTickerMessage{}, fmt.Errorf("missing field %q", "data")
	}
	sticker, err := parseSpotTicker(data)
	if err != nil {
		return SpotTickerMessage{}, err
	}
	return SpotTickerMessage{
		Topic:         string(v.GetStringBytes("topic")),
		Timestamp:     v.GetInt64("ts"),
		Type:          string(v.GetStringBytes("type")),
		CrossSequence: v.GetInt64("cs"),
		Data:          sticker,
	}, nil
}

func parsePriceLevel(v *fastjson.Value) (tradekit.Level, error) {
	priceS := string(v.GetStringBytes("0"))
	price, err := strconv.ParseFloat(priceS, 64)
	if err != nil {
		return tradekit.Level{}, fmt.Errorf("invalid level price %q", priceS)

	}
	amountS := string(v.GetStringBytes("1"))
	amount, err := strconv.ParseFloat(amountS, 64)
	if err != nil {
		return tradekit.Level{}, fmt.Errorf("invalid level amount %q", amountS)
	}
	return tradekit.Level{Price: price, Amount: amount}, nil
}

func parsePriceLevels(items []*fastjson.Value) ([]tradekit.Level, error) {
	levels := make([]tradekit.Level, len(items))
	for i, v := range items {
		level, err := parsePriceLevel(v)
		if err != nil {
			return nil, err
		}
		levels[i] = level
	}
	return levels, nil
}

func parseOrderbookUpdate(v *fastjson.Value) (OrderbookUpdate, error) {
	bids, err := parsePriceLevels(v.GetArray("b"))
	if err != nil {
		return OrderbookUpdate{}, fmt.Errorf("parsing field %q: %w", "b", err)
	}

	asks, err := parsePriceLevels(v.GetArray("a"))
	if err != nil {
		return OrderbookUpdate{}, fmt.Errorf("parsing field %q: %w", "a", err)
	}

	return OrderbookUpdate{
		Symbol:   string(v.GetStringBytes("s")),
		Bids:     bids,
		Asks:     asks,
		UpdateID: v.GetInt64("u"),
		Sequence: v.GetInt64("seq"),
	}, nil

}

func parseOrderbookUpdateMessage(v *fastjson.Value) (OrderbookUpdateMessage, error) {
	data := v.Get("data")
	if data == nil {
		return OrderbookUpdateMessage{}, fmt.Errorf("missing field %q", "data")
	}

	odata, err := parseOrderbookUpdate(data)
	if err != nil {
		return OrderbookUpdateMessage{}, err
	}

	return OrderbookUpdateMessage{
		Topic:     string(v.GetStringBytes("topic")),
		Type:      string(v.GetStringBytes("type")),
		Timestamp: v.GetInt64("ts"),
		Data:      odata,
	}, nil
}

func parseTrade(v *fastjson.Value) (Trade, error) {
	return Trade{
		Timestamp:  v.GetInt64("T"),
		Symbol:     string(v.GetStringBytes("s")),
		Price:      conv.BytesToFloat(v.GetStringBytes("p")),
		Amount:     conv.BytesToFloat(v.GetStringBytes("v")),
		Direction:  string(v.GetStringBytes("S")),
		TradeID:    string(v.GetStringBytes("i")),
		BlockTrade: v.GetBool("BT"),
	}, nil

}

func parseTradesMessage(v *fastjson.Value) (TradesMessage, error) {
	data := v.GetArray("data")
	if data == nil {
		return TradesMessage{}, fmt.Errorf("missing field %q", "data")
	}

	trades := make([]Trade, len(data))
	for i, item := range data {
		trade, err := parseTrade(item)
		if err != nil {
			return TradesMessage{}, err
		}
		trades[i] = trade
	}

	return TradesMessage{
		Topic:     string(v.GetStringBytes("topic")),
		Type:      string(v.GetStringBytes("type")),
		Timestamp: v.GetInt64("ts"),
		Data:      trades,
	}, nil
}

func parseLiquidationData(v *fastjson.Value) (LiquidationData, error) {
	priceS := string(v.GetStringBytes("price"))
	price, err := strconv.ParseFloat(priceS, 64)
	if err != nil {
		return LiquidationData{}, fmt.Errorf("invalid price %q", priceS)
	}

	amountS := string(v.GetStringBytes("size"))
	amount, err := strconv.ParseFloat(amountS, 64)
	if err != nil {
		return LiquidationData{}, fmt.Errorf("invalid size %q", amountS)
	}

	return LiquidationData{
		Price:       price,
		Amount:      amount,
		Direction:   string(v.GetStringBytes("side")),
		Symbol:      string(v.GetStringBytes("symbol")),
		UpdatedTime: v.GetInt64("updatedTime"),
	}, nil
}

func parseLiquidation(v *fastjson.Value) (Liquidation, error) {
	data, err := parseLiquidationData(v.Get("data"))
	if err != nil {
		return Liquidation{}, err
	}
	return Liquidation{
		Topic:     string(v.GetStringBytes("topic")),
		Timestamp: v.GetInt64("ts"),
		Type:      string(v.GetStringBytes("type")),
		Data:      data,
	}, nil
}
