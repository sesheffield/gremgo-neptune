package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gedge/graphson"
	"github.com/gedge/gremgo-neptune"
)

func main() {
	errs := make(chan error)
	go func(errs chan error) {
		err := <-errs
		log.Fatal("Lost connection to the database: " + err.Error())
	}(errs) // Example of connection error handling logic

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1*time.Minute))
	pool := gremgo.NewPoolWithDialerCtx(ctx, "ws://127.0.0.1:8182/gremlin", errs)

	cursor, err := pool.OpenCursorCtx( // Sends a query to Gremlin Server with bindings
		ctx,
		"g.V().limit(10000)",
		nil, nil,
	)
	if err != nil {
		fmt.Println(err)
		return
	}

	var verts []graphson.Vertex
	var label string
	count := 0
	for eof := false; !eof; {
		verts, eof, err = pool.ReadCursorCtx(ctx, cursor)
		if err != nil {
			fmt.Println(err)
			return
		}
		if len(verts) > 0 {
			count += len(verts)
			fmt.Printf("%+v and %d more...\n", verts[0], len(verts)-1)
			if label == "" {
				label, err = verts[0].GetLabel()
				if err != nil {
					fmt.Println(err)
					return
				}
			}
		}
		select {
		case ctxErr := <-ctx.Done():
			fmt.Println(ctxErr)
			return
		default:
		}
	}

	cancel()
	fmt.Printf("Total %d vertices. Label for first: %q\n", count, label)
}
