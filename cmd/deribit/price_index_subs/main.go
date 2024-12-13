package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/antibubblewrap/tradekit/deribit"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	priceIndexSubs := []deribit.PriceIndexSub{
		{IndexName: "btc_usd"},
		{IndexName: "eth_usd"},
	}
	priceIndexStream := deribit.NewPriceIndexStream("wss://streams.deribit.com/ws/api/v2", priceIndexSubs...)

	if err := priceIndexStream.Start(ctx); err != nil {
		panic(err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-priceIndexStream.Messages():
			fmt.Printf("%+v\n", msg)
		case err := <-priceIndexStream.Err():
			fmt.Printf("error %s", err)
			panic(err)
		}
	}
}
