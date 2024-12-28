package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"

	"github.com/bogdanovich/tradekit/deribit"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	tickerSubs := []deribit.DeribitTickerSub{
		{Instrument: "BTC-PERPETUAL", Interval: "agg2"},
		//{Instrument: "ETH-3JAN25-3300-P", Interval: "agg2"},
	}
	tickerStream := deribit.NewTickerStream("wss://streams.deribit.com/ws/api/v2", tickerSubs)

	if err := tickerStream.Start(ctx); err != nil {
		panic(err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-tickerStream.Messages():
			//fmt.Printf("%+v\n", msg)
			json, _ := json.Marshal(msg)
			fmt.Printf("%s\n", string(json))
		case err := <-tickerStream.Err():
			fmt.Printf("error %s", err)
			panic(err)
		}
	}
}
