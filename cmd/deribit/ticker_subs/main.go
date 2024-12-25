package main

import (
	"context"
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
	}
	tickerStream := deribit.NewTickerStream("wss://streams.deribit.com/ws/api/v2", tickerSubs...)

	if err := tickerStream.Start(ctx); err != nil {
		panic(err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-tickerStream.Messages():
			fmt.Printf("%+v\n", msg)
		case err := <-tickerStream.Err():
			fmt.Printf("error %s", err)
			panic(err)
		}
	}
}
