package deribit

import (
	"bytes"
	"time"

	"github.com/bogdanovich/tradekit"
	"github.com/valyala/fastjson"
)



type PublicTrade struct {
	TradeSeq      int64   `json:"trade_seq" parquet:"name=trade_seq, type=INT64"`
	Timestamp     int64   `json:"timestamp" parquet:"name=timestamp, type=INT64"`
	Instrument    string  `json:"instrument_name" parquet:"name=instrument, type=BYTE_ARRAY, convertedtype=UTF8"`
	Price         float64 `json:"price" parquet:"name=price, type=DOUBLE"`
	Amount        float64 `json:"amount" parquet:"name=amount, type=DOUBLE"`
	Direction     string  `json:"direction" parquet:"name=direction, type=BYTE_ARRAY, convertedtype=UTF8"`
	TickDirection int     `json:"tick_direction" parquet:"name=tick_direction, type=INT32"` // Direction of the "tick" (0 = Plus Tick, 1 = Zero-Plus Tick, 2 = Minus Tick, 3 = Zero-Minus Tick).
	TradeId       string  `json:"trade_id" parquet:"name=trade_id, type=BYTE_ARRAY, convertedtype=UTF8"`
	Liquidation   string  `json:"liquidation" parquet:"name=liquidation, type=BYTE_ARRAY, convertedtype=UTF8"`
}

// InstrumentState is the type of message produced by the InstrumentStateStream.
// For details, see: https://docs.deribit.com/#instrument-state-kind-currency
type InstrumentState struct {
	Timestamp  int64  `json:"timestamp"`
	State      string `json:"state"`
	Instrument string `json:"instrument_name"`
}

type OrderbookDepth struct {
	Timestamp  int64            `json:"timestamp" parquet:"name=timestamp, type=INT64"`
	Instrument string           `json:"instrument_name" parquet:"name=instrument_name, type=BYTE_ARRAY, convertedtype=UTF8"`
	ChangeID   int64            `json:"change_id" parquet:"name=change_id, type=INT64"`
	Bids       []tradekit.Level `json:"bids" parquet:"name=bids, type=LIST"`
	Asks       []tradekit.Level `json:"asks" parquet:"name=asks, type=LIST"`
}

// OrderbookMsg is the message type streamed from the Deribit book channel.
// For more info see: https://docs.deribit.com/#book-instrument_name-interval
type OrderbookUpdate struct {
	// The type of orderbook update. Either "snapshot" or "change".
	Type         string           `parquet:"name=type, type=BYTE_ARRAY, convertedtype=UTF8"`
	Timestamp    int64            `parquet:"name=timestamp, type=INT64"`
	Instrument   string           `parquet:"name=instrument, type=BYTE_ARRAY, convertedtype=UTF8"`
	ChangeID     int64            `parquet:"name=change_id, type=INT64"`
	PrevChangeID int64            `parquet:"name=prev_change_id, type=INT64"`
	Bids         []tradekit.Level `parquet:"name=bids, type=LIST"`
	Asks         []tradekit.Level `parquet:"name=asks, type=LIST"`
}

func parseOrderbookUpdate(v *fastjson.Value) OrderbookUpdate {
	return OrderbookUpdate{
		Type:         string(v.GetStringBytes("type")),
		Timestamp:    v.GetInt64("timestamp"),
		Instrument:   string(v.GetStringBytes("instrument_name")),
		ChangeID:     v.GetInt64("change_id"),
		PrevChangeID: v.GetInt64("prev_change_id"),
		Bids:         parseOrderbookLevels(v.GetArray("bids")),
		Asks:         parseOrderbookLevels(v.GetArray("asks")),
	}
}

func parseOrderbookLevel(v *fastjson.Value) tradekit.Level {
	action := v.GetStringBytes("0")
	price := v.GetFloat64("1")
	amount := v.GetFloat64("2")
	if bytes.Equal(action, []byte("delete")) {
		amount = 0
	}
	return tradekit.Level{Price: price, Amount: amount}
}

func parseOrderbookLevels(items []*fastjson.Value) []tradekit.Level {
	levels := make([]tradekit.Level, len(items))
	for i, item := range items {
		levels[i] = parseOrderbookLevel(item)
	}
	return levels
}

func parsePublicTrade(v *fastjson.Value) PublicTrade {
	return PublicTrade{
		TradeSeq:      v.GetInt64("trade_seq"),
		Timestamp:     v.GetInt64("timestamp"),
		Instrument:    string(v.GetStringBytes("instrument_name")),
		Price:         v.GetFloat64("price"),
		Amount:        v.GetFloat64("amount"),
		Direction:     string(v.GetStringBytes("direction")),
		TickDirection: v.GetInt("tick_direction"),
		TradeId:       string(v.GetStringBytes("trade_id")),
		Liquidation:   string(v.GetStringBytes("liquidation")),
	}
}

func parsePublicTrades(v *fastjson.Value) []PublicTrade {
	items := v.GetArray()
	trades := make([]PublicTrade, len(items))
	for i, item := range items {
		trades[i] = parsePublicTrade(item)
	}
	return trades
}

func parseInstrumentState(v *fastjson.Value) InstrumentState {
	return InstrumentState{
		Timestamp:  v.GetInt64("timestamp"),
		State:      string(v.GetStringBytes("state")),
		Instrument: string(v.GetStringBytes("instrument_name")),
	}
}

func parseOrderbookDepth(v *fastjson.Value) OrderbookDepth {
	return OrderbookDepth{
		Timestamp:  v.GetInt64("timestamp"),
		Instrument: string(v.GetStringBytes("instrument_name")),
		ChangeID:   v.GetInt64("change_id"),
		Bids:       parsePriceLevels(v.GetArray("bids")),
		Asks:       parsePriceLevels(v.GetArray("asks")),
	}
}

func parsePriceLevel(v *fastjson.Value) tradekit.Level {
	price := v.GetFloat64("0")
	amount := v.GetFloat64("1")
	return tradekit.Level{Price: price, Amount: amount}
}

func parsePriceLevels(items []*fastjson.Value) []tradekit.Level {
	levels := make([]tradekit.Level, len(items))
	for i, v := range items {
		levels[i] = parsePriceLevel(v)
	}
	return levels
}

// Option is the type of a Deribit option instrument.
// For more details see https://docs.deribit.com/#public-get_instrument
type Option struct {
	Name                     string         `json:"instrument_name"`
	Strike                   float64        `json:"strike"`
	OptionType               string         `json:"option_type"`
	IsActive                 bool           `json:"is_active"`
	ExpirationTimestamp      int64          `json:"expiration_timestamp"`
	CreationTimestamp        int64          `json:"creation_timestamp"`
	BaseCurrency             string         `json:"base_currency"`
	QuoteCurrency            string         `json:"quote_currency"`
	CounterCurrency          string         `json:"counter_currency"`
	SettlementCurrency       string         `json:"settlement_currency"`
	TickSize                 float64        `json:"tick_size"`
	TickSizeSteps            []TickSizeStep `json:"tick_size_steps"`
	TakerCommission          float64        `json:"taker_commission"`
	MakerCommission          float64        `json:"maker_commission"`
	SettlementPeriod         string         `json:"settlement_period"`
	RFQ                      bool           `json:"rfq"`
	PriceIndex               string         `json:"price_index"`
	MinTradeAmount           float64        `json:"min_trade_amount"`
	InstrumentId             int64          `json:"instrument_id"`
	ContractSize             float64        `json:"contract_size"`
	BlockTradeCommission     float64        `json:"block_trade_commission,omitempty"`
	BlockTradeMinTradeAmount float64        `json:"block_trade_min_trade_amount,omitempty"`
	BlockTradeTickSize       float64        `json:"block_trade_tick_size,omitempty"`
}

// DeribitPosition represents the state of a user's position in an instrument as returned by
// [TradeExecutor.GetPosition] or [TradeExecutor.GetPositions].
type DeribitPosition struct {
	AveragePrice              float64        `json:"average_price"`
	AveragePriceUSD           float64        `json:"average_price_usd"`
	Delta                     float64        `json:"delta"`
	Direction                 string         `json:"direction"`
	EstimatedLiquidationPrice float64        `json:"estimated_liquidation_price"`
	FloatingProfitLoss        float64        `json:"floating_profit_loss"`
	FloatingProfitLossUSD     float64        `json:"floating_profit_loss_usd"`
	Gamma                     float64        `json:"gamma"`
	IndexPrice                float64        `json:"index_price"`
	InitialMargin             float64        `json:"initial_margin"`
	InstrumentName            string         `json:"instrument_name"`
	InterestValue             float64        `json:"interest_value"`
	Kind                      InstrumentKind `json:"kind"`
	Leverage                  int            `json:"leverage"`
	MaintenanceMargin         float64        `json:"maintenance_margin"`
	MarkPrice                 float64        `json:"mark_price"`
	OpenOrdersMargin          float64        `json:"open_orders_margin"`
	RealizedFunding           float64        `json:"realized_funding"`
	RealizedProfitLoss        float64        `json:"realized_profit_loss"`
	SettlementPrice           float64        `json:"settlement_price"`
	Size                      float64        `json:"size"`
	SizeCurrency              float64        `json:"size_currency"`
	Theta                     float64        `json:"theta"`
	TotalProfitLoss           float64        `json:"total_profit_loss"`
	Vega                      float64        `json:"vega"`
}

func parsePosition(v *fastjson.Value) DeribitPosition {
	return DeribitPosition{
		AveragePrice:              v.GetFloat64("average_price"),
		AveragePriceUSD:           v.GetFloat64("average_price_usd"),
		Delta:                     v.GetFloat64("delta"),
		Direction:                 string(v.GetStringBytes("direction")),
		EstimatedLiquidationPrice: v.GetFloat64("estimated_liquidation_price"),
		FloatingProfitLoss:        v.GetFloat64("floating_profit_loss"),
		FloatingProfitLossUSD:     v.GetFloat64("floating_profit_loss_usd"),
		Gamma:                     v.GetFloat64("gamma"),
		IndexPrice:                v.GetFloat64("index_price"),
		InitialMargin:             v.GetFloat64("initial_margin"),
		InstrumentName:            string(v.GetStringBytes("instrument_name")),
		InterestValue:             v.GetFloat64("interest_value"),
		Kind:                      InstrumentKind(v.GetStringBytes("kind")),
		Leverage:                  v.GetInt("leverage"),
		MaintenanceMargin:         v.GetFloat64("maintenance_margin"),
		MarkPrice:                 v.GetFloat64("mark_price"),
		OpenOrdersMargin:          v.GetFloat64("open_orders_margin"),
		RealizedFunding:           v.GetFloat64("realized_funding"),
		RealizedProfitLoss:        v.GetFloat64("realized_profit_loss"),
		SettlementPrice:           v.GetFloat64("settlement_price"),
		Size:                      v.GetFloat64("size"),
		SizeCurrency:              v.GetFloat64("size_currency"),
		Theta:                     v.GetFloat64("theta"),
		TotalProfitLoss:           v.GetFloat64("total_profit_loss"),
		Vega:                      v.GetFloat64("vega"),
	}
}

func parsePositions(v *fastjson.Value) []DeribitPosition {
	items := v.GetArray()
	positions := make([]DeribitPosition, len(items))
	for i, item := range items {
		positions[i] = parsePosition(item)
	}
	return positions
}

type TickSizeStep struct {
	AbovePrice float64 `json:"above_price"`
	TickSize   float64 `json:"tick_size"`
}

// CurrencyInfo is the type returned from the Deribit /public/get_currencies endpoint.
// For details see https://docs.deribit.com/#public-get_currencies
type CurrencyInfo struct {
	CoinType         string  `json:"coin_type"`
	Currency         string  `json:"currency"`
	CurrencyLong     string  `json:"currency_long"`
	FeePrecision     int     `json:"fee_precision"`
	MinConfirmations int     `json:"min_confirmations"`
	WithdrawalFee    float64 `json:"withdrawal_fee"`
}

type DeliveryPrice struct {
	Date  string  `json:"date"`
	Price float64 `json:"delivery_price"`
}

type deliveryPrices struct {
	Prices       []DeliveryPrice `json:"data"`
	RecordsTotal int             `json:"records_total"`
}

type OptionsGetDeliveryPrices struct {
	Count int
}

// GetTradesOptions define optional parameters for retrieving trades.
type GetTradesOptions struct {
	// StartTimestamp and EndTimestamp define the timeframe over which trades will be
	// returned. If StartTimestamp is specfied then trades will be returned in ascending
	// order, otherwise they will be returned in descending order.
	StartTimestamp time.Time
	EndTimestamp   time.Time

	// Count defines the number of trades to return per pagination request. If unspecified,
	// it defaults to 10
	Count int

	// We set these internally to know what method to use.
	currency   string
	kind       InstrumentKind
	instrument string

	// We use these for pagination.
	startTradeId  string
	endTradeId    string
	startSequence int
	endSequence   int
}

// Instrument holds all fields returned in the "result" array for each instrument.
type Instrument struct {
	BaseCurrency             string         `json:"base_currency"`
	BlockTradeCommission     float64        `json:"block_trade_commission"`
	BlockTradeMinTradeAmount float64        `json:"block_trade_min_trade_amount"`
	BlockTradeTickSize       float64        `json:"block_trade_tick_size"`
	ContractSize             float64        `json:"contract_size"`
	CounterCurrency          string         `json:"counter_currency"`
	CreationTimestamp        int64          `json:"creation_timestamp"`
	ExpirationTimestamp      int64          `json:"expiration_timestamp"`
	FutureType               string         `json:"future_type"` // Deprecated; use InstrumentType instead.
	InstrumentID             int            `json:"instrument_id"`
	InstrumentName           string         `json:"instrument_name"`
	InstrumentType           string         `json:"instrument_type"`
	IsActive                 bool           `json:"is_active"`
	Kind                     string         `json:"kind"`
	MakerCommission          float64        `json:"maker_commission"`
	MaxLeverage              int            `json:"max_leverage"`
	MaxLiquidationCommission float64        `json:"max_liquidation_commission"`
	MinTradeAmount           float64        `json:"min_trade_amount"`
	OptionType               string         `json:"option_type"`
	PriceIndex               string         `json:"price_index"`
	QuoteCurrency            string         `json:"quote_currency"`
	Rfq                      bool           `json:"rfq"`
	SettlementCurrency       string         `json:"settlement_currency"`
	SettlementPeriod         string         `json:"settlement_period"`
	Strike                   float64        `json:"strike"`
	TakerCommission          float64        `json:"taker_commission"`
	TickSize                 float64        `json:"tick_size"`
	TickSizeSteps            []TickSizeStep `json:"tick_size_steps"`
}

func parseInstrument(v *fastjson.Value) Instrument {
	return Instrument{
		BaseCurrency:             string(v.GetStringBytes("base_currency")),
		BlockTradeCommission:     v.GetFloat64("block_trade_commission"),
		BlockTradeMinTradeAmount: v.GetFloat64("block_trade_min_trade_amount"),
		BlockTradeTickSize:       v.GetFloat64("block_trade_tick_size"),
		ContractSize:             v.GetFloat64("contract_size"),
		CounterCurrency:          string(v.GetStringBytes("counter_currency")),
		CreationTimestamp:        v.GetInt64("creation_timestamp"),
		ExpirationTimestamp:      v.GetInt64("expiration_timestamp"),
		FutureType:               string(v.GetStringBytes("future_type")),
		InstrumentID:             v.GetInt("instrument_id"),
		InstrumentName:           string(v.GetStringBytes("instrument_name")),
		InstrumentType:           string(v.GetStringBytes("instrument_type")),
		IsActive:                 v.GetBool("is_active"),
		Kind:                     string(v.GetStringBytes("kind")),
		MakerCommission:          v.GetFloat64("maker_commission"),
		MaxLeverage:              v.GetInt("max_leverage"),
		MaxLiquidationCommission: v.GetFloat64("max_liquidation_commission"),
		MinTradeAmount:           v.GetFloat64("min_trade_amount"),
		OptionType:               string(v.GetStringBytes("option_type")),
		PriceIndex:               string(v.GetStringBytes("price_index")),
		QuoteCurrency:            string(v.GetStringBytes("quote_currency")),
		Rfq:                      v.GetBool("rfq"),
		SettlementCurrency:       string(v.GetStringBytes("settlement_currency")),
		SettlementPeriod:         string(v.GetStringBytes("settlement_period")),
		Strike:                   v.GetFloat64("strike"),
		TakerCommission:          v.GetFloat64("taker_commission"),
		TickSize:                 v.GetFloat64("tick_size"),
		TickSizeSteps:            parseTickSizeSteps(v.GetArray("tick_size_steps")),
	}
}

func parseTickSizeStep(v *fastjson.Value) TickSizeStep {
	return TickSizeStep{
		AbovePrice: v.GetFloat64("above_price"),
		TickSize:   v.GetFloat64("tick_size"),
	}
}

func parseTickSizeSteps(items []*fastjson.Value) []TickSizeStep {
	steps := make([]TickSizeStep, len(items))
	for i, item := range items {
		steps[i] = parseTickSizeStep(item)
	}
	return steps
}

// BookSummary represents the summary information for an instrument's orderbook
// For details see: https://docs.deribit.com/#public-get_book_summary_by_instrument
type BookSummary struct {
	AskPrice              float64 `json:"ask_price"`
	BaseCurrency          string  `json:"base_currency"`
	BidPrice             float64 `json:"bid_price"`
	CreationTimestamp    int64   `json:"creation_timestamp"`
	CurrentFunding       float64 `json:"current_funding"`
	EstDeliveryPrice    float64 `json:"estimated_delivery_price"`
	Funding8h           float64 `json:"funding_8h"`
	High                float64 `json:"high"`
	InstrumentName      string  `json:"instrument_name"`
	InterestRate        float64 `json:"interest_rate"`
	Last                float64 `json:"last"`
	Low                 float64 `json:"low"`
	MarkIV              float64 `json:"mark_iv"`
	MarkPrice           float64 `json:"mark_price"`
	MidPrice            float64 `json:"mid_price"`
	OpenInterest        float64 `json:"open_interest"`
	PriceChange         float64 `json:"price_change"`
	QuoteCurrency       string  `json:"quote_currency"`
	UnderlyingIndex     string  `json:"underlying_index"`
	UnderlyingPrice     float64 `json:"underlying_price"`
	Volume              float64 `json:"volume"`
	VolumeNotional     float64 `json:"volume_notional"`
	VolumeUSD          float64 `json:"volume_usd"`
}

func parseBookSummary(v *fastjson.Value) BookSummary {
	return BookSummary{
		AskPrice:           v.GetFloat64("ask_price"),
		BaseCurrency:       string(v.GetStringBytes("base_currency")),
		BidPrice:          v.GetFloat64("bid_price"),
		CreationTimestamp: v.GetInt64("creation_timestamp"),
		CurrentFunding:    v.GetFloat64("current_funding"),
		EstDeliveryPrice: v.GetFloat64("estimated_delivery_price"),
		Funding8h:        v.GetFloat64("funding_8h"),
		High:             v.GetFloat64("high"),
		InstrumentName:   string(v.GetStringBytes("instrument_name")),
		InterestRate:     v.GetFloat64("interest_rate"),
		Last:             v.GetFloat64("last"),
		Low:              v.GetFloat64("low"),
		MarkIV:           v.GetFloat64("mark_iv"),
		MarkPrice:        v.GetFloat64("mark_price"),
		MidPrice:         v.GetFloat64("mid_price"),
		OpenInterest:     v.GetFloat64("open_interest"),
		PriceChange:      v.GetFloat64("price_change"),
		QuoteCurrency:    string(v.GetStringBytes("quote_currency")),
		UnderlyingIndex:  string(v.GetStringBytes("underlying_index")),
		UnderlyingPrice:  v.GetFloat64("underlying_price"),
		Volume:           v.GetFloat64("volume"),
		VolumeNotional:  v.GetFloat64("volume_notional"),
		VolumeUSD:       v.GetFloat64("volume_usd"),
	}
}

func parseBookSummaries(v *fastjson.Value) []BookSummary {
	items := v.GetArray()
	summaries := make([]BookSummary, len(items))
	for i, item := range items {
		summaries[i] = parseBookSummary(item)
	}
	return summaries
}
