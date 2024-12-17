package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/antibubblewrap/tradekit/deribit"
	"github.com/antibubblewrap/tradekit/lib/tk"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	priceIndexSubs := []deribit.DeribitPriceIndexSub{
		{IndexName: "btc_usd"},
		{IndexName: "eth_usd"},
		//{IndexName: "usdt_usd"},
	}
	priceIndexStream := deribit.NewPriceIndexStream(
		"wss://streams.deribit.com/ws/api/v2",
		priceIndexSubs,
		tk.WithLogger(tk.NewLogger()),
	)

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
