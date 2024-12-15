package deribit

import (
	"fmt"

	"github.com/antibubblewrap/tradekit/lib/tk"
	"github.com/valyala/fastjson"
)

// UserPortfolioSub represents a subscription to a Deribit user portfolio stream.
// For details see: https://docs.deribit.com/#user-portfolio-currency
type UserPortfolioSub struct {
	Currency string // e.g., BTC, ETH, etc.
}

type UserPortfolioData struct {
	Currency                     string
	MaintenanceMargin            float64
	DeltaTotal                   float64
	OptionsSessionRPL            float64
	FuturesSessionRPL            float64
	DeltaTotalMap                map[string]float64
	SessionUPL                   float64
	FeeBalance                   float64
	EstimatedLiquidationRatio    float64
	InitialMargin                float64
	OptionsGammaMap              map[string]float64
	FuturesPL                    float64
	OptionsValue                 float64
	ProjectedMaintenanceMargin   float64
	OptionsVega                  float64
	SessionRPL                   float64
	FuturesSessionUPL            float64
	OptionsSessionUPL            float64
	CrossCollateralEnabled       bool
	OptionsTheta                 float64
	MarginModel                  string
	OptionsDelta                 float64
	OptionsPL                    float64
	OptionsVegaMap               map[string]float64
	Balance                      float64
	AdditionalReserve            float64
	EstimatedLiquidationRatioMap map[string]float64
	ProjectedInitialMargin       float64
	AvailableFunds               float64
	ProjectedDeltaTotal          float64
	PortfolioMarginingEnabled    bool
	TotalPL                      float64
	MarginBalance                float64
	OptionsThetaMap              map[string]float64
	AvailableWithdrawalFunds     float64
	Equity                       float64
	OptionsGamma                 float64
}

// Returns the channel name for the given currency.
func (sub UserPortfolioSub) channel() string {
	return fmt.Sprintf("user.portfolio.%s", sub.Currency)
}

// NewUserPortfolioStream creates a new [Stream] which produces a stream of user portfolio updates.
// For details see: https://docs.deribit.com/#user-portfolio-currency
func NewUserPortfolioStream(wsUrl string, c Credentials, subscriptions []UserPortfolioSub, paramFuncs ...tk.Param) Stream[UserPortfolioData, UserPortfolioSub] {
	p := streamParams[UserPortfolioData, UserPortfolioSub]{
		name:         "user_portfolio_stream",
		wsUrl:        wsUrl,
		isPrivate:    true, // Portfolio stream requires authentication
		parseMessage: parsePortfolioData,
		subs:         subscriptions,
		Params:       tk.ApplyParams(paramFuncs),
	}
	s := newStream[UserPortfolioData](p)
	s.SetCredentials(&c)
	return s
}

func parsePortfolioData(v *fastjson.Value) UserPortfolioData {
	return UserPortfolioData{
		Currency:                     string(v.GetStringBytes("currency")),
		MaintenanceMargin:            v.GetFloat64("maintenance_margin"),
		DeltaTotal:                   v.GetFloat64("delta_total"),
		OptionsSessionRPL:            v.GetFloat64("options_session_rpl"),
		FuturesSessionRPL:            v.GetFloat64("futures_session_rpl"),
		DeltaTotalMap:                parseFloatMap(v.Get("delta_total_map")),
		SessionUPL:                   v.GetFloat64("session_upl"),
		FeeBalance:                   v.GetFloat64("fee_balance"),
		EstimatedLiquidationRatio:    v.GetFloat64("estimated_liquidation_ratio"),
		InitialMargin:                v.GetFloat64("initial_margin"),
		OptionsGammaMap:              parseFloatMap(v.Get("options_gamma_map")),
		FuturesPL:                    v.GetFloat64("futures_pl"),
		OptionsValue:                 v.GetFloat64("options_value"),
		ProjectedMaintenanceMargin:   v.GetFloat64("projected_maintenance_margin"),
		OptionsVega:                  v.GetFloat64("options_vega"),
		SessionRPL:                   v.GetFloat64("session_rpl"),
		FuturesSessionUPL:            v.GetFloat64("futures_session_upl"),
		OptionsSessionUPL:            v.GetFloat64("options_session_upl"),
		CrossCollateralEnabled:       v.GetBool("cross_collateral_enabled"),
		OptionsTheta:                 v.GetFloat64("options_theta"),
		MarginModel:                  string(v.GetStringBytes("margin_model")),
		OptionsDelta:                 v.GetFloat64("options_delta"),
		OptionsPL:                    v.GetFloat64("options_pl"),
		OptionsVegaMap:               parseFloatMap(v.Get("options_vega_map")),
		Balance:                      v.GetFloat64("balance"),
		AdditionalReserve:            v.GetFloat64("additional_reserve"),
		EstimatedLiquidationRatioMap: parseFloatMap(v.Get("estimated_liquidation_ratio_map")),
		ProjectedInitialMargin:       v.GetFloat64("projected_initial_margin"),
		AvailableFunds:               v.GetFloat64("available_funds"),
		ProjectedDeltaTotal:          v.GetFloat64("projected_delta_total"),
		PortfolioMarginingEnabled:    v.GetBool("portfolio_margining_enabled"),
		TotalPL:                      v.GetFloat64("total_pl"),
		MarginBalance:                v.GetFloat64("margin_balance"),
		OptionsThetaMap:              parseFloatMap(v.Get("options_theta_map")),
		AvailableWithdrawalFunds:     v.GetFloat64("available_withdrawal_funds"),
		Equity:                       v.GetFloat64("equity"),
		OptionsGamma:                 v.GetFloat64("options_gamma"),
	}
}

func parseFloatMap(v *fastjson.Value) map[string]float64 {
	if v == nil {
		return nil
	}

	result := make(map[string]float64)
	v.GetObject().Visit(func(key []byte, value *fastjson.Value) {
		result[string(key)] = value.GetFloat64()
	})
	return result
}
