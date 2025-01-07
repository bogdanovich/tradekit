package bybit

import (
	"encoding/json"
	"testing"

	"github.com/bogdanovich/tradekit"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fastjson"
)

func TestParseSpotTicker(t *testing.T) {
	input := `
	{
    "topic": "tickers.ETHUSDT",
    "ts": 1736220203217,
    "type": "snapshot",
    "cs": 89486329596,
    "data": {
        "symbol": "ETHUSDT",
        "lastPrice": "3679.25",
        "highPrice24h": "3745.7",
        "lowPrice24h": "3622.1",
        "prevPrice24h": "3660.26",
        "volume24h": "215730.42023",
        "turnover24h": "792650578.4000126",
        "price24hPcnt": "0.0052",
        "usdIndexPrice": "3680.151282"
    }
}
	`

	expected := SpotTicker{
		Topic:         "tickers.ETHUSDT",
		Type:          "snapshot",
		Timestamp:     1736220203217,
		CrossSequence: 89486329596,
		Data: SpotTickerData{
			Symbol:        "ETHUSDT",
			LastPrice:     3679.25,
			HighPrice24h:  3745.7,
			LowPrice24h:   3622.1,
			PrevPrice24h:  3660.26,
			Volume24h:     215730.42023,
			Turnover24h:   792650578.4000126,
			Price24hPcnt:  0.0052,
			USDIndexPrice: 3680.151282,
		},
	}

	var p fastjson.Parser
	v, err := p.Parse(input)
	assert.Nil(t, err)
	ticker := ParseSpotTicker(v)
	assert.Equal(t, expected, ticker)
}

func TestMarshalUnmarshalSpotTicker(t *testing.T) {
	expected := SpotTicker{
		Topic:         "tickers.ETHUSDT",
		Type:          "snapshot",
		Timestamp:     1736220203217,
		CrossSequence: 89486329596,
		Data: SpotTickerData{
			Symbol:        "ETHUSDT",
			LastPrice:     3679.25,
			HighPrice24h:  3745.7,
			LowPrice24h:   3622.1,
			PrevPrice24h:  3660.26,
			Volume24h:     215730.42023,
			Turnover24h:   792650578.4000126,
			Price24hPcnt:  0.0052,
			USDIndexPrice: 3680.151282,
		},
	}

	json, err := json.Marshal(expected)
	assert.Nil(t, err)

	var p fastjson.Parser
	v, err := p.Parse(string(json))
	assert.Nil(t, err)
	ticker := ParseSpotTicker(v)
	assert.Equal(t, expected, ticker)
}

func TestParseOrderbookUpdte(t *testing.T) {
	input := `
	{
		"topic": "orderbook.50.BTCUSDT",
		"type": "snapshot",
		"ts": 1672304484978,
		"data": {
			"s": "BTCUSDT",
			"b": [
				[
					"16493.50",
					"0.006"
				],
				[
					"16493.00",
					"0.100"
				]
			],
			"a": [
				[
					"16611.00",
					"0.029"
				],
				[
					"16612.00",
					"0.213"
				]
			],
		"u": 18521288,
		"seq": 7961638724
		}
	}	
	`

	expected := OrderbookUpdateMessage{
		Topic:     "orderbook.50.BTCUSDT",
		Type:      "snapshot",
		Timestamp: 1672304484978,
		Data: OrderbookUpdate{
			Symbol: "BTCUSDT",
			Bids: []tradekit.Level{
				{Price: 16493.50, Amount: 0.006},
				{Price: 16493.00, Amount: 0.100},
			},
			Asks: []tradekit.Level{
				{Price: 16611.00, Amount: 0.029},
				{Price: 16612.00, Amount: 0.213},
			},
			UpdateID: 18521288,
			Sequence: 7961638724,
		},
	}

	var p fastjson.Parser
	v, err := p.Parse(input)
	assert.Nil(t, err)
	msg := ParseOrderbookUpdateMessage(v)
	assert.Equal(t, expected, msg)
}

func TestParseLiquidation(t *testing.T) {
	input := `
	{
		"data": {
			"price": "0.03803",
			"side": "Buy",
			"size": "1637",
			"symbol": "GALAUSDT",
			"updatedTime": 1673251091822
		},
		"topic": "liquidation.GALAUSDT",
		"ts": 1673251091822,
		"type": "snapshot"
	}	
	`
	expected := Liquidation{
		Data: LiquidationData{
			Price:       0.03803,
			Direction:   Buy,
			Amount:      1637,
			Symbol:      "GALAUSDT",
			UpdatedTime: 1673251091822,
		},
		Topic:     "liquidation.GALAUSDT",
		Timestamp: 1673251091822,
		Type:      "snapshot",
	}

	var p fastjson.Parser
	v, err := p.Parse(input)
	assert.Nil(t, err)
	msg := ParseLiquidation(v)
	assert.Equal(t, expected, msg)

}

func TestMarshalUnmarshalTrade(t *testing.T) {
	expected := Trade{
		Timestamp:  1673251091822,
		Symbol:     "BTCUSDT",
		Price:      3679.25,
		Amount:     0.5,
		Direction:  "Buy",
		TradeID:    123456789,
		BlockTrade: true,
	}

	jsonData, err := json.Marshal(expected)
	assert.Nil(t, err)

	var p fastjson.Parser
	v, err := p.Parse(string(jsonData))
	assert.Nil(t, err)
	trade := ParseTrade(v)
	assert.Equal(t, expected, trade)
}
