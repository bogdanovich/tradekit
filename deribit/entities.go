package deribit

import (
	"time"

	"github.com/valyala/fastjson"
)

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
