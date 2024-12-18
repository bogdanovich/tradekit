package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/antibubblewrap/tradekit/deribit"
)

func main() {
	_, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	api, err := deribit.NewApi("https://www.deribit.com/api/v2/")
	if err != nil {
		panic(err)
	}

	positions, err := api.GetPositions(deribit.GetPositionsParams{
		Credentials: deribit.Credentials{
			ClientId:     os.Getenv("DERIBIT_CLIENT_ID"),
			ClientSecret: os.Getenv("DERIBIT_CLIENT_SECRET"),
		},
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(positions)

}
