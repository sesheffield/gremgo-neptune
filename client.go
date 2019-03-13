package gremgo

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/gedge/gremgo-neptune/graphson"
	gutil "github.com/gedge/gremgo-neptune/utils"
	"github.com/pkg/errors"
)

var ErrorConnectionDisposed = errors.New("you cannot write on a disposed connection")

// Client is a container for the gremgo client.
type Client struct {
	conn             dialer
	requests         chan []byte
	responses        chan []byte
	results          *sync.Map
	responseNotifier *sync.Map // responseNotifier notifies the requester that a response has been completed for the request
	chunkNotifier    *sync.Map // chunkNotifier contains channels per requestID (if using cursors) which notifies the requester that a partial response has arrived
	mu               sync.RWMutex
	Errored          bool
}

// NewDialer returns a WebSocket dialer to use when connecting to Gremlin Server
func NewDialer(host string, configs ...DialerConfig) (dialer *Ws) {
	dialer = &Ws{
		timeout:      15 * time.Second,
		pingInterval: 60 * time.Second,
		writingWait:  15 * time.Second,
		readingWait:  15 * time.Second,
		connected:    false,
		quit:         make(chan struct{}),
	}

	for _, conf := range configs {
		conf(dialer)
	}

	dialer.host = host
	return dialer
}

func newClient() (c Client) {
	c.requests = make(chan []byte, 3)  // c.requests takes any request and delivers it to the WriteWorker for dispatch to Gremlin Server
	c.responses = make(chan []byte, 3) // c.responses takes raw responses from ReadWorker and delivers it for sorting to handleResponse
	c.results = &sync.Map{}
	c.responseNotifier = &sync.Map{}
	c.chunkNotifier = &sync.Map{}
	return
}

// Dial returns a gremgo client for interaction with the Gremlin Server specified in the host IP.
func Dial(conn dialer, errs chan error) (c Client, err error) {
	return DialCtx(context.Background(), conn, errs)
}

// DialCtx returns a gremgo client for interaction with the Gremlin Server specified in the host IP.
func DialCtx(ctx context.Context, conn dialer, errs chan error) (c Client, err error) {
	c = newClient()
	c.conn = conn

	// Connects to Gremlin Server
	err = conn.connectCtx(ctx)
	if err != nil {
		return
	}

	// quit := conn.(*Ws).quit
	msgChan := make(chan []byte, 200)

	go c.writeWorkerCtx(ctx, errs)
	go c.readWorkerCtx(ctx, msgChan, errs)
	go c.saveWorkerCtx(ctx, msgChan, errs)
	// go c.readWorker(errs, quit)
	go conn.pingCtx(ctx, errs)

	return
}

func (c *Client) executeRequest(query string, bindings, rebindings map[string]string) (resp []Response, err error) {
	return c.executeRequestCtx(context.Background(), query, bindings, rebindings)
}
func (c *Client) executeRequestCtx(ctx context.Context, query string, bindings, rebindings map[string]string) (resp []Response, err error) {
	var req request
	var id string
	req, id, err = prepareRequest(query, bindings, rebindings)
	if err != nil {
		return
	}

	msg, err := packageRequest(req)
	if err != nil {
		log.Println(err)
		return
	}
	c.responseNotifier.Store(id, make(chan error, 1))
	c.dispatchRequestCtx(ctx, msg)
	resp, err = c.retrieveResponseCtx(ctx, id)
	if err != nil {
		err = errors.Wrapf(err, "query: %s", query)
	}
	return
}
func (c *Client) executeRequestCursorCtx(ctx context.Context, query string, bindings, rebindings map[string]string) (id string, err error) {
	var req request
	if req, id, err = prepareRequest(query, bindings, rebindings); err != nil {
		return
	}

	var msg []byte
	if msg, err = packageRequest(req); err != nil {
		log.Println(err)
		return
	}
	c.responseNotifier.Store(id, make(chan error, 1))
	c.chunkNotifier.Store(id, make(chan bool, 10))
	c.dispatchRequestCtx(ctx, msg)
	return id, nil
}

func (c *Client) authenticate(requestID string) (err error) {
	auth := c.conn.getAuth()
	req, err := prepareAuthRequest(requestID, auth.username, auth.password)
	if err != nil {
		return
	}

	msg, err := packageRequest(req)
	if err != nil {
		log.Println(err)
		return
	}

	c.dispatchRequest(msg)
	return
}

// Execute formats a raw Gremlin query, sends it to Gremlin Server, and returns the result.
func (c *Client) Execute(query string, bindings, rebindings map[string]string) (resp []Response, err error) {
	if c.conn.isDisposed() {
		return resp, ErrorConnectionDisposed
	}
	resp, err = c.executeRequest(query, bindings, rebindings)
	return
}

// ExecuteFile takes a file path to a Gremlin script, sends it to Gremlin Server, and returns the result.
func (c *Client) ExecuteFile(path string, bindings, rebindings map[string]string) (resp []Response, err error) {
	if c.conn.isDisposed() {
		return resp, ErrorConnectionDisposed
	}
	d, err := ioutil.ReadFile(path) // Read script from file
	if err != nil {
		log.Println(err)
		return
	}
	query := string(d)
	resp, err = c.executeRequest(query, bindings, rebindings)
	return
}

// Get formats a raw Gremlin query, sends it to Gremlin Server, and populates the passed []interface.
func (c *Client) Get(query string, ptr interface{}) (res []graphson.Vertex, err error) {
	return c.GetCtx(context.Background(), query, ptr)
}
func (c *Client) GetCtx(ctx context.Context, query string, ptr interface{}) (res []graphson.Vertex, err error) {
	if c.conn.isDisposed() {
		err = ErrorConnectionDisposed
		return
	}

	gutil.Dump("Get Q ", query)
	var resp []Response
	resp, err = c.executeRequestCtx(ctx, query, nil, nil)
	if err != nil {
		return
	}
	gutil.Dump("GetRes ", resp)
	return c.deserializeResponseToVertices(resp)
}

func (c *Client) deserializeResponseToVertices(resp []Response) (res []graphson.Vertex, err error) {
	if len(resp) == 0 || resp[0].Status.Code == statusNoContent {
		// gutil.Warn("deserializeR2V: no results - status: %s", resp[0].Status.Code)
		return
	}

	for _, item := range resp {
		resN, err := graphson.DeserializeListOfVerticesFromBytes(item.Result.Data)
		if err != nil {
			panic(err)
		}
		// resN := make([]graphson.Vertex, 1)
		// gutil.Dump(fmt.Sprintf("DLoV%02d ", idx), resN)
		res = append(res, resN...)
	}
	// 	gutil.Dump("GetZ ", strct)
	return
}

// GetCursorCtx initiates a query on the database, returning a cursor to iterate over the results
func (c *Client) GetCursorCtx(ctx context.Context, query string, ptr interface{}) (respID string, err error) {
	if c.conn.isDisposed() {
		err = ErrorConnectionDisposed
		return
	}

	gutil.Dump("GetCurs Q ", query)
	respID, err = c.executeRequestCursorCtx(ctx, query, nil, nil)
	if err != nil {
		return
	}
	gutil.Dump("GetCurs Res ", respID)
	return
}

// NextCursorCtx returns the next set of results for the cursor
// - `res` may be empty when results were read by a previous call
// - `eof` will be true when no more results are available
func (c *Client) NextCursorCtx(ctx context.Context, cursor string) (res []graphson.Vertex, eof bool, err error) {
	var resp []Response
	if resp, eof, err = c.retrieveNextResponseCtx(ctx, cursor); err != nil {
		err = errors.Wrapf(err, "cursor: %s", cursor)
		return
	}

	res, err = c.deserializeResponseToVertices(resp)
	return
}

// GetE formats a raw Gremlin query, sends it to Gremlin Server, and populates the passed []interface.
func (c *Client) GetE(query string) (res graphson.Edges, err error) {
	if c.conn.isDisposed() {
		err = ErrorConnectionDisposed
		return
	}

	gutil.Dump("GetEq ", query)
	resp, err := c.executeRequest(query, nil, nil)
	if err != nil {
		return
	}
	gutil.Dump("GetERes ", resp)
	if len(resp) == 0 || resp[0].Status.Code == statusNoContent {
		gutil.Warn("GetE: no results")
		return
	}

	for idx, item := range resp {
		var resN graphson.Edges
		if resN, err = graphson.DeserializeListOfEdgesFromBytes(item.Result.Data); err != nil {
			return
		}
		gutil.Dump(fmt.Sprintf("DLoE%02d ", idx), resN)
		res = append(res, resN...)
	}

	return
}

// GremlinForVertex returns the addV()... and V()... gremlin commands for `data`
// Because of possible multiples, it does not start with `g.` (it probably should XXX )
func GremlinForVertex(label string, data interface{}) (gremAdd, gremGet string, err error) {

	d := reflect.ValueOf(data)
	var id reflect.Value
	var missingId bool
	if id = d.FieldByName("Id"); !id.IsValid() {
		missingId = true
		// err = errors.New("the passed interface must have an Id field")
		// return
	}

	gremAdd = fmt.Sprintf("addV('%s')", label)
	gremGet = fmt.Sprintf("V('%s')", label)

	if !missingId {
		gremAdd = fmt.Sprintf("%s.property(id,'%s')", gremAdd, id)
		gremGet = fmt.Sprintf("%s.hasId('%s')", gremGet, id)
	}

	// missingTag := true

	for i := 0; i < d.NumField(); i++ {
		tag := d.Type().Field(i).Tag.Get("graph")
		name, opts := parseTag(tag)
		if len(name) == 0 && len(opts) == 0 {
			// gutil.Warn("no opts for field %q with label %q data %+v", d.Type().Field(i).Name, label, data)
			continue
		}
		// missingTag = false
		val := d.Field(i).Interface()
		if len(opts) == 0 {
			err = fmt.Errorf("interface field tag does not contain a tag option type, field type: %T", val)
			return
		}
		if !d.Field(i).IsValid() {
			gutil.Warn("invalid field for label %q name %q data %+v", label, name, data)
			continue
		}
		if opts.Contains("id") {
			if val != "" {
				gremAdd = fmt.Sprintf("%s.property(id,'%s')", gremAdd, val)
				gremGet = fmt.Sprintf("%s.hasId('%s')", gremGet, val)
			}
		} else if opts.Contains("string") {
			if val != "" {
				gremAdd = fmt.Sprintf("%s.property('%s','%s')", gremAdd, name, val)
				gremGet = fmt.Sprintf("%s.has('%s','%s')", gremGet, name, val)
			}
		} else if opts.Contains("bool") || opts.Contains("number") || opts.Contains("other") {
			gremAdd = fmt.Sprintf("%s.property('%s',%v)", gremAdd, name, val)
			gremGet = fmt.Sprintf("%s.has('%s',%v)", gremGet, name, val)
		} else if opts.Contains("[]string") {
			s := reflect.ValueOf(val)
			for i := 0; i < s.Len(); i++ {
				gremAdd = fmt.Sprintf("%s.property('%s','%s')", gremAdd, name, s.Index(i).Interface())
				gremGet = fmt.Sprintf("%s.has('%s','%s')", gremGet, name, s.Index(i).Interface())
			}
		} else if opts.Contains("[]bool") || opts.Contains("[]number") || opts.Contains("[]other") {
			s := reflect.ValueOf(val)
			for i := 0; i < s.Len(); i++ {
				gremAdd = fmt.Sprintf("%s.property('%s',%v)", gremAdd, name, s.Index(i).Interface())
				gremGet = fmt.Sprintf("%s.has('%s',%v)", gremGet, name, s.Index(i).Interface())
			}
		} else {
			err = fmt.Errorf("interface field tag needs recognised option, field: %q, tag: %q", d.Type().Field(i).Name, tag)
			return
		}
	}

	// if missingTag {
	// 	err = fmt.Errorf("interface of type: %T, does not contain any graph tags", data)
	// 	return
	// }
	return
}

// AddV takes a label and an interface and adds it as a vertex to the graph
func (c *Client) AddV(label string, data interface{}) (vert graphson.Vertex, err error) {
	if c.conn.isDisposed() {
		return vert, ErrorConnectionDisposed
	}

	q, _, err := GremlinForVertex(label, data)
	if err != nil {
		panic(err) // XXX
	}
	q = "g." + q

	gutil.Dump("addvq ", q)
	var resp []Response
	resp, err = c.Execute(q, nil, nil)
	if err != nil {
		panic(err) // XXX
	}

	if len(resp) != 1 {
		return vert, fmt.Errorf("AddV should receive 1 response, got %d", len(resp))
	}

	for idx, res := range resp { // XXX one result, so do not need this
		result, err := graphson.DeserializeListOfVerticesFromBytes(res.Result.Data)
		if err != nil {
			panic(err) // XXX
		}
		if len(result) != 1 {
			return vert, fmt.Errorf("AddV should receive 1 result, got %d", len(result))
		}

		gutil.Dump(fmt.Sprintf("aV%02d ", idx), result)
		vert = result[0]
	}
	return
}

// AddE takes a label, from UUID and to UUID then creates a edge between the two vertex in the graph
func (c *Client) AddE(label string, fromId, toId string) (resp interface{}, err error) {
	if c.conn.isDisposed() {
		return nil, ErrorConnectionDisposed
	}

	q := fmt.Sprintf("g.addE('%s').from(g.V().hasId('%s')).to(g.V().hasId('%s')).property('%s','%s')", label, fromId, toId, "ook", "foo")
	gutil.Warn(q)
	resp, err = c.Execute(q, nil, nil)
	return

}

// Close closes the underlying connection and marks the client as closed.
func (c *Client) Close() {
	if c.conn != nil {
		c.conn.close()
	}
}
