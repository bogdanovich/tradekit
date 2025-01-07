package bybit

import (
	"encoding/json"
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

type Trades struct {
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
	TradeID    int64   `json:"i" parquet:"name=trade_id, type=INT64"`
	BlockTrade bool    `json:"BT" parquet:"name=block_trade, type=BOOLEAN"`
}

// MarshalJSON implements the json.Marshaler interface.
// It marshals the Trade to a JSON string with float64 values as strings.
func (t Trade) MarshalJSON() ([]byte, error) {
	type Alias Trade
	return json.Marshal(&struct {
		Price  string `json:"p"`
		Amount string `json:"v"`
		*Alias
	}{
		Price:  strconv.FormatFloat(t.Price, 'f', -1, 64),
		Amount: strconv.FormatFloat(t.Amount, 'f', -1, 64),
		Alias:  (*Alias)(&t),
	})
}

type SpotTicker struct {
	Topic         string         `json:"topic" parquet:"name=topic, type=BYTE_ARRAY, convertedtype=UTF8"` // Topic name
	Timestamp     int64          `json:"ts" parquet:"name=timestamp, type=INT64"`                         // Timestamp (ms) the system generates the data
	Type          string         `json:"type" parquet:"name=type, type=BYTE_ARRAY, convertedtype=UTF8"`   // Data type: snapshot
	CrossSequence int64          `json:"cs" parquet:"name=cross_sequence, type=INT64"`                    // Cross sequence
	Data          SpotTickerData `json:"data" parquet:"name=data"`                                        // Ticker data
}

type SpotTickerData struct {
	Symbol        string  `json:"symbol" parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8"`
	LastPrice     float64 `json:"lastPrice" parquet:"name=last_price, type=DOUBLE"`
	HighPrice24h  float64 `json:"highPrice24h" parquet:"name=high_price_24h, type=DOUBLE"`
	LowPrice24h   float64 `json:"lowPrice24h" parquet:"name=low_price_24h, type=DOUBLE"`
	PrevPrice24h  float64 `json:"prevPrice24h" parquet:"name=prev_price_24h, type=DOUBLE"`
	Volume24h     float64 `json:"volume24h" parquet:"name=volume_24h, type=DOUBLE"`
	Turnover24h   float64 `json:"turnover24h" parquet:"name=turnover_24h, type=DOUBLE"`
	Price24hPcnt  float64 `json:"price24hPcnt" parquet:"name=price_24h_pcnt, type=DOUBLE"`
	USDIndexPrice float64 `json:"usdIndexPrice" parquet:"name=usd_index_price, type=DOUBLE"`
}

// MarshalJSON implements the json.Marshaler interface.
// It marshals the SpotTickerData to a JSON string with float64 values as strings.
func (s SpotTickerData) MarshalJSON() ([]byte, error) {
	type Alias SpotTickerData
	return json.Marshal(&struct {
		LastPrice     string `json:"lastPrice"`
		HighPrice24h  string `json:"highPrice24h"`
		LowPrice24h   string `json:"lowPrice24h"`
		PrevPrice24h  string `json:"prevPrice24h"`
		Volume24h     string `json:"volume24h"`
		Turnover24h   string `json:"turnover24h"`
		Price24hPcnt  string `json:"price24hPcnt"`
		USDIndexPrice string `json:"usdIndexPrice"`
		*Alias
	}{
		LastPrice:     strconv.FormatFloat(s.LastPrice, 'f', -1, 64),
		HighPrice24h:  strconv.FormatFloat(s.HighPrice24h, 'f', -1, 64),
		LowPrice24h:   strconv.FormatFloat(s.LowPrice24h, 'f', -1, 64),
		PrevPrice24h:  strconv.FormatFloat(s.PrevPrice24h, 'f', -1, 64),
		Volume24h:     strconv.FormatFloat(s.Volume24h, 'f', -1, 64),
		Turnover24h:   strconv.FormatFloat(s.Turnover24h, 'f', -1, 64),
		Price24hPcnt:  strconv.FormatFloat(s.Price24hPcnt, 'f', -1, 64),
		USDIndexPrice: strconv.FormatFloat(s.USDIndexPrice, 'f', -1, 64),
		Alias:         (*Alias)(&s),
	})
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

func parseSpotTickerData(v *fastjson.Value) (SpotTickerData, error) {
	return SpotTickerData{
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

func ParseSpotTicker(v *fastjson.Value) SpotTicker {
	data := v.Get("data")
	if data == nil {
		return SpotTicker{}
	}
	sticker, err := parseSpotTickerData(data)
	if err != nil {
		return SpotTicker{}
	}
	return SpotTicker{
		Topic:         string(v.GetStringBytes("topic")),
		Timestamp:     v.GetInt64("ts"),
		Type:          string(v.GetStringBytes("type")),
		CrossSequence: v.GetInt64("cs"),
		Data:          sticker,
	}
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

func ParseOrderbookUpdate(v *fastjson.Value) OrderbookUpdate {
	bids, err := parsePriceLevels(v.GetArray("b"))
	if err != nil {
		return OrderbookUpdate{}
	}

	asks, err := parsePriceLevels(v.GetArray("a"))
	if err != nil {
		return OrderbookUpdate{}
	}

	return OrderbookUpdate{
		Symbol:   string(v.GetStringBytes("s")),
		Bids:     bids,
		Asks:     asks,
		UpdateID: v.GetInt64("u"),
		Sequence: v.GetInt64("seq"),
	}
}

func ParseOrderbookUpdateMessage(v *fastjson.Value) OrderbookUpdateMessage {
	data := v.Get("data")
	if data == nil {
		return OrderbookUpdateMessage{}
	}

	odata := ParseOrderbookUpdate(data)

	return OrderbookUpdateMessage{
		Topic:     string(v.GetStringBytes("topic")),
		Type:      string(v.GetStringBytes("type")),
		Timestamp: v.GetInt64("ts"),
		Data:      odata,
	}
}

func ParseTrade(v *fastjson.Value) Trade {
	return Trade{
		Timestamp:  v.GetInt64("T"),
		Symbol:     string(v.GetStringBytes("s")),
		Price:      conv.BytesToFloat(v.GetStringBytes("p")),
		Amount:     conv.BytesToFloat(v.GetStringBytes("v")),
		Direction:  string(v.GetStringBytes("S")),
		TradeID:    int64(v.GetInt64("i")),
		BlockTrade: v.GetBool("BT"),
	}

}

func ParseTradesMessage(v *fastjson.Value) Trades {
	data := v.GetArray("data")
	if data == nil {
		return Trades{}
	}

	trades := make([]Trade, len(data))
	for i, item := range data {
		trades[i] = ParseTrade(item)
	}

	return Trades{
		Topic:     string(v.GetStringBytes("topic")),
		Type:      string(v.GetStringBytes("type")),
		Timestamp: v.GetInt64("ts"),
		Data:      trades,
	}
}

func ParseLiquidationData(v *fastjson.Value) LiquidationData {
	priceS := string(v.GetStringBytes("price"))
	price, err := strconv.ParseFloat(priceS, 64)
	if err != nil {
		return LiquidationData{}
	}

	amountS := string(v.GetStringBytes("size"))
	amount, err := strconv.ParseFloat(amountS, 64)
	if err != nil {
		return LiquidationData{}
	}

	return LiquidationData{
		Price:       price,
		Amount:      amount,
		Direction:   string(v.GetStringBytes("side")),
		Symbol:      string(v.GetStringBytes("symbol")),
		UpdatedTime: v.GetInt64("updatedTime"),
	}
}

func ParseLiquidation(v *fastjson.Value) Liquidation {
	data := ParseLiquidationData(v.Get("data"))
	return Liquidation{
		Topic:     string(v.GetStringBytes("topic")),
		Timestamp: v.GetInt64("ts"),
		Type:      string(v.GetStringBytes("type")),
		Data:      data,
	}
}
