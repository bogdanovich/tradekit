package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/bogdanovich/tradekit/deribit"
	"github.com/bogdanovich/tradekit/lib/tk"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	userPortfolioSubs := []deribit.DeribitUserPortfolioSub{
		{Currency: "any"},
	}
	userPortfolioStream := deribit.NewUserPortfolioStream(
		"wss://www.deribit.com/ws/api/v2",
		userPortfolioSubs,
		tk.WithCredentials(tk.Credentials{
			ClientId:     os.Getenv("DERIBIT_CLIENT_ID"),
			ClientSecret: os.Getenv("DERIBIT_CLIENT_SECRET"),
		}),
		tk.WithLogger(tk.NewLogger()),
	)

	if err := userPortfolioStream.Start(ctx); err != nil {
		panic(err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-userPortfolioStream.Messages():
			fmt.Printf("%+v\n", msg)
		case err := <-userPortfolioStream.Err():
			fmt.Printf("error %s", err)
			panic(err)
		}
	}
}
