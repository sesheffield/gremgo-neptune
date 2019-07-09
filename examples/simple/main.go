package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/ONSdigital/gremgo-neptune"
)

func main() {
	errs := make(chan error)
	go func(errs chan error) {
		err := <-errs
		log.Fatal("Lost connection to the database: " + err.Error())
	}(errs) // Example of connection error handling logic

	dialer := gremgo.NewDialer("ws://127.0.0.1:8182/gremlin") // Returns a WebSocket dialer to connect to Gremlin Server
	g, err := gremgo.Dial(dialer, errs)                       // Returns a gremgo client to interact with
	if err != nil {
		fmt.Println(err)
		return
	}
	res, err := g.Execute( // Sends a query to Gremlin Server with bindings
		"g.V('1234')", nil, nil,
	)
	if err != nil {
		fmt.Println(err)
		return
	}
	j, err := json.Marshal(res[0].Result.Data) // res will return a list of resultsets,  where the data is a json.RawMessage
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%s\n", j)
}
