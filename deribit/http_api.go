package deribit

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/bogdanovich/tradekit/lib/safe"
)

// Error is returned by requests to the Deribit API in the event that the request
// was not successful. For details see https://docs.deribit.com/#json-rpc
type Error struct {
	Code    int
	Message string
}

func (e Error) Error() string {
	return fmt.Sprintf("Deribit RPC error [%d]: %s", e.Code, e.Message)
}

// Iterator allows for iteration over paginated methods on the Deribit API.
type Iterator[T any] interface {
	// Done returns true when there are no more results in the iterator.
	Done() bool
	// Next returns the next batch of items from the iterator. You should stop calling
	// Next after Done returns true.
	Next() (T, error)
}

// Api allows for sending requests to the Deribit JSON-RPC API over HTTP.
type Api struct {
	baseUrl *url.URL
}

// NewApi creates a new Api to either the prod or testing Deribit server.
func NewApi(apiUrl string) (*Api, error) {
	baseUrl, err := url.Parse(apiUrl)
	if err != nil {
		return nil, err
	}
	return &Api{baseUrl: baseUrl}, nil
}

func apiPublicGet[T any](api *Api, method rpcMethod, params map[string]string) (T, error) {
	u := urlWithParams(api.baseUrl, method, params)
	r, err := http.Get(u)
	if err != nil {
		var zero T
		return zero, apiErr(method, err)
	}
	defer r.Body.Close()

	var resp RpcResponse[T]
	if err := json.NewDecoder(r.Body).Decode(&resp); err != nil {
		var zero T
		return zero, apiErr(method, err)
	}
	if resp.Error != nil {
		var zero T
		return zero, apiErr(method, *resp.Error)
	}

	return resp.Result, nil
}

func apiPrivateGet[T any](api *Api, method rpcMethod, params map[string]string, c Credentials) (T, error) {
	u := urlWithParams(api.baseUrl, method, params)
	req, err := http.NewRequest("GET", u, nil)
	req.SetBasicAuth(c.ClientId, c.ClientSecret)
	if err != nil {
		var zero T
		return zero, err
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		var zero T
		return zero, apiErr(method, err)
	}
	defer r.Body.Close()

	var resp RpcResponse[T]
	if err := json.NewDecoder(r.Body).Decode(&resp); err != nil {
		var zero T
		return zero, apiErr(method, err)
	}
	if resp.Error != nil {
		var zero T
		return zero, apiErr(method, *resp.Error)
	}

	return resp.Result, nil
}

type GetPositionsParams struct {
	Credentials  Credentials
	SubaccountID *string
	Currency     *string
	Kind         *InstrumentKind
}

// GetPositions retrieves all positions for specific currency, kind, and subaccount_id.
func (api *Api) GetPositions(p GetPositionsParams) ([]DeribitPosition, error) {
	params := map[string]string{}
	if p.Currency != nil {
		params["currency"] = *p.Currency
	}
	if p.Kind != nil {
		params["kind"] = string(*p.Kind)
	}
	return apiPrivateGet[[]DeribitPosition](api, methodPrivateGetPositions, params, p.Credentials)
}

// GetOptionInstruments retrieves all Deribit option instruments on the given currency.
// Set expired to true to show recently expired options instead of active ones.
func (api *Api) GetOptionInstruments(currency string, expired bool) ([]Option, error) {
	exp := "false"
	if expired {
		exp = "true"
	}
	params := map[string]string{"currency": currency, "kind": "option", "expired": exp}
	return apiPublicGet[[]Option](api, methodPublicGetInstruments, params)
}

type GetInstrumentsParams struct {
	Currency string
	Kind     *string
	Expired  *bool
}

// GetOptionInstruments retrieves all Deribit option instruments on the given currency.
// Set expired to true to show recently expired options instead of active ones.
func (api *Api) GetInstruments(p GetInstrumentsParams) ([]Instrument, error) {
	params := map[string]string{
		"currency": p.Currency,
		"expired":  strconv.FormatBool(safe.Bool(p.Expired)),
	}
	if p.Kind != nil {
		params["kind"] = *p.Kind
	}
	return apiPublicGet[[]Instrument](api, methodPublicGetInstruments, params)
}

// GetCurrencies returns a slice of the supported currencies on Deribit from the
// /public/get_currencies endpoint.
// For details see https://docs.deribit.com/#public-get_currencies
func (api *Api) GetCurrencies() ([]CurrencyInfo, error) {
	return apiPublicGet[[]CurrencyInfo](api, methodPublicGetCurrencies, nil)
}

type deliveryPricesIterator struct {
	done      bool
	api       *Api
	count     int
	offset    int
	indexName string
}

func (it *deliveryPricesIterator) Done() bool {
	return it.done
}

func (it *deliveryPricesIterator) Next() ([]DeliveryPrice, error) {
	method := methodPublicGetDeliveryPrices
	params := map[string]string{"index_name": it.indexName, "offset": strconv.Itoa(it.offset), "count": strconv.Itoa(it.count)}
	resp, err := apiPublicGet[deliveryPrices](it.api, method, params)
	if err != nil {
		return nil, apiErr(method, err)
	}

	if len(resp.Prices) == 0 {
		it.done = true
	} else {
		it.offset += len(resp.Prices)
	}

	return resp.Prices, nil
}

// GetDeliveryPrices returns an iterator over delivery prices for a given index.
// For more details see: https://docs.deribit.com/#public-get_delivery_prices. Results
// are returned in descending order from the most recent delivery price.
func (api *Api) GetDeliveryPrices(indexName string, p *OptionsGetDeliveryPrices) Iterator[[]DeliveryPrice] {
	count := 10
	if p != nil && p.Count != 0 {
		count = p.Count
	}
	return &deliveryPricesIterator{api: api, count: count, indexName: indexName}
}

type IndexPrice struct {
	EstimatedDeliveryPrice float64 `json:"estimated_delivery_price"`
	IndexPrice             float64 `json:"index_price"`
}

// GetIndexPrice returns the current price of a given index from the /public/get_index_price
// endpoint.
func (api *Api) GetIndexPrice(indexName string) (IndexPrice, error) {
	params := map[string]string{"index_name": indexName}
	return apiPublicGet[IndexPrice](api, methodPublicGetIndexPrice, params)
}

func (p GetTradesOptions) methodAndParams() (rpcMethod, map[string]string, error) {
	params := make(map[string]string)
	var method rpcMethod
	if p.currency != "" {
		params["currency"] = p.currency
		if p.startTradeId != "" || p.endTradeId != "" {
			method = methodPublicGetLastTradesByCurrency
			if p.startTradeId != "" {
				params["start_id"] = p.startTradeId
			} else {
				params["end_id"] = p.endTradeId
			}
		} else {
			if !p.StartTimestamp.IsZero() && !p.EndTimestamp.IsZero() {
				method = methodPublicGetLastTradesByCurrencyAndTime
			} else {
				method = methodPublicGetLastTradesByCurrency
			}
		}
		if p.kind != "" {
			params["kind"] = string(p.kind)
		}
	} else if p.instrument != "" {
		params["instrument_name"] = p.instrument
		if p.startSequence != 0 || p.endSequence != 0 {
			method = methodPublicGetLastTradesByInstrument
			if p.startSequence != 0 {
				params["start_seq"] = strconv.Itoa(p.startSequence)
			} else {
				params["end_seq"] = strconv.Itoa(p.endSequence)
			}
		} else {
			if !p.StartTimestamp.IsZero() && !p.EndTimestamp.IsZero() {
				method = methodPublicGetLastTradesByInstrumentAndTime
			} else {
				method = methodPublicGetLastTradesByInstrument
			}
		}
	} else {
		panic("GetTradesOptions methodAndParams unreachable!")
	}

	if p.isIterDescending() {
		params["sorting"] = "desc"
	} else {
		params["sorting"] = "asc"
	}

	if !p.StartTimestamp.IsZero() && p.startSequence == 0 && p.startTradeId == "" {
		params["start_timestamp"] = strconv.Itoa(int(p.StartTimestamp.UnixMilli()))
	}
	if !p.EndTimestamp.IsZero() {
		params["end_timestamp"] = strconv.Itoa(int(p.EndTimestamp.UnixMilli()))
	}

	if p.Count != 0 {
		params["count"] = strconv.Itoa(p.Count)
	} else {
		params["count"] = "10"
	}

	return method, params, nil
}

func (p GetTradesOptions) isIterDescending() bool {
	return p.StartTimestamp.IsZero()
}

type getTradesResponse struct {
	Trades  []PublicTrade `json:"trades"`
	HasMore bool          `json:"has_more"`
}

type tradesIterator struct {
	done      bool
	params    GetTradesOptions
	api       *Api
	doneFirst bool
}

func (it *tradesIterator) Next() ([]PublicTrade, error) {
	method, params, err := it.params.methodAndParams()
	if err != nil {
		it.done = true
		return nil, err
	}
	resp, err := apiPublicGet[getTradesResponse](it.api, method, params)
	if err != nil {
		it.done = true
		return nil, apiErr(method, err)
	}

	trades := resp.Trades
	if it.doneFirst {
		// After the first request, we skip the first trade. This is because subsequent
		// requests set either the start trade Id, or start sequence (depending it we're
		// querying the currency or instrument), and we need to exclude it, otherwise
		// the first trade will be a duplicate
		trades = trades[1:]
	}

	if len(trades) == 0 {
		it.done = true
		return trades, nil
	}

	if resp.HasMore {
		t := trades[len(trades)-1]
		if it.params.isIterDescending() {
			if it.params.currency != "" {
				it.params.endTradeId = t.TradeId
			} else if it.params.instrument != "" {
				it.params.endSequence = int(t.TradeSeq)
			}
		} else {
			if it.params.currency != "" {
				it.params.startTradeId = t.TradeId
			} else if it.params.instrument != "" {
				it.params.startSequence = int(t.TradeSeq)
			}
		}
	} else {
		it.done = true
	}

	it.doneFirst = true

	return trades, nil
}

func (it *tradesIterator) Done() bool {
	return it.done
}

func (api *Api) GetLastTradesByCurrencyAndKind(currency string, kind InstrumentKind, opts *GetTradesOptions) Iterator[[]PublicTrade] {
	return api.getLastTrades("", currency, kind, opts)
}

func (api *Api) GetLastTradesByInstrument(instrument string, opts *GetTradesOptions) Iterator[[]PublicTrade] {
	return api.getLastTrades(instrument, "", "", opts)
}

func (api *Api) getLastTrades(instrument string, currency string, kind InstrumentKind, opts *GetTradesOptions) Iterator[[]PublicTrade] {
	var options GetTradesOptions
	if opts == nil {
		options.Count = 10
	} else {
		if opts.Count != 0 {
			options.Count = 10
		}
		if !opts.StartTimestamp.IsZero() {
			options.StartTimestamp = opts.StartTimestamp
		}
		if !opts.EndTimestamp.IsZero() {
			options.EndTimestamp = opts.EndTimestamp
		}
	}
	if instrument != "" {
		options.instrument = instrument
	} else {
		options.currency = currency
		options.kind = kind
	}
	return &tradesIterator{api: api, params: options}
}

func urlWithParams(baseUrl *url.URL, method rpcMethod, params map[string]string) string {
	u := baseUrl.JoinPath(string(method))
	values := url.Values{}
	for k, v := range params {
		values.Set(k, v)
	}
	u.RawQuery = values.Encode()
	return u.String()
}

func apiErr(method rpcMethod, err error) error {
	return fmt.Errorf("Deribit API error %s: %w", method, err)
}

// GetBookSummaryByCurrency retrieves the summary information for all instruments of a currency
// For details see: https://docs.deribit.com/#public-get_book_summary_by_currency
func (api *Api) GetBookSummaryByCurrency(currency string, kind InstrumentKind) ([]BookSummary, error) {
	params := map[string]string{
		"currency": currency,
		"kind":     string(kind),
	}
	return apiPublicGet[[]BookSummary](api, methodPublicGetBookSummaryByCurrency, params)
}
