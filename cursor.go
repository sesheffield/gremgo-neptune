package gremgo

import (
	"context"
	"io"
	"sync"

	"github.com/gedge/graphson"
	"github.com/pkg/errors"
)

// Cursor allows for results to be iterated over as soon as available, rather than waiting for
// a query to complete and all results to be returned in one block.
type Cursor struct {
	ID     string
	mu     sync.RWMutex
	eof    bool
	buffer []string
	client *Client
}

func (c *Cursor) Read() (string, error) {
	if len(c.buffer) > 0 {
		s := c.buffer[0] + "\n"

		if len(c.buffer) > 1 {
			c.buffer = c.buffer[1:]
		} else {
			c.buffer = []string{}
		}
		return s, nil
	}

	var resp []Response
	var err error

	var attempts int
	for resp == nil && !c.eof || attempts > 5 { //resp could be empty if reading too quickly
		attempts++
		if resp, c.eof, err = c.client.retrieveNextResponseCtx(context.Background(), c); err != nil {
			err = errors.Wrapf(err, "cursor.Read: %s", c.ID)
			return "", err
		}
	}

	if c.eof || (len(resp) == 1 && &resp[0].Status != nil && resp[0].Status.Code == 204) {
		return "", io.EOF
	}

	if c.buffer, err = graphson.DeserializeStringListFromBytes(resp[0].Result.Data); err != nil {
		return "", err
	}

	if len(c.buffer) == 0 {
		return "", errors.New("no results deserialized")
	}

	return c.Read()
}

func (c *Cursor) Close(ctx context.Context) error {
	return nil
}
