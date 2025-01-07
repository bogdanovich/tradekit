package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"

	"github.com/bogdanovich/tradekit/bybit"
)

func main() {
	// Connects to the ByBit order book stream for a symbol and outputs the messages to
	// a file "bybit_orderbook_stream.jsonl".

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	bybitUrl := "wss://stream.bybit.com/v5/public/linear"
	subs := []bybit.TickerSub{
		{Symbol: "BTC-24JAN25"},
	}
	bybitTickerStream := bybit.NewFuturesTickerStream(bybitUrl, subs)
	if err := bybitTickerStream.Start(ctx); err != nil {
		panic(err)
	}

	f, err := os.Create("bybit_spot_tickers_stream.jsonl")
	if err != nil {
		panic(err)
	}
	fb := bufio.NewWriter(f)
	defer func() {
		fb.Flush()
		f.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-bybitTickerStream.Messages():
			// m := bookUpdate{msg.Type, msg.Data.Bids, msg.Data.Asks}
			data, err := json.Marshal(msg)
			if err != nil {
				panic(err)
			}
			if _, err := fb.Write(data); err != nil {
				panic(err)
			}
			fmt.Println(msg)
			fb.WriteString("\n")
		case err := <-bybitTickerStream.Err():
			panic(err)
		}
	}

}
