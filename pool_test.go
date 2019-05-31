package gremgo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gedge/graphson"
	"github.com/pkg/errors"
)

var dummyDialFunc func() (*Client, error)

func MockNewPoolWithDialerCtx(ctx context.Context, dbURL string, errs chan error, t *testing.T, expectReq []byte, response Response) *Pool {
	mockDBResponseChan := make(chan Response, 10)
	mockDialFunc := func() (*Client, error) {
		var cli Client
		var err error
		dialerMocked := &dialerMock{
			connectCtxFunc: func(context.Context) error { return nil },
			writeFunc: func(req []byte) error {
				// obtain first request-id from client keys (should be only one)
				var id string
				cli.responseNotifier.Range(func(key, val interface{}) bool {
					id = key.(string)
					return false
				})

				// replace requestId value in req with (generic) "<reqid>" to facilitate comparison with expected
				req = bytes.Replace(req, []byte(`"requestId":"`+id+`"`), []byte(`"requestId":"<reqid>"`), 1)

				if len(req) != len(expectReq) || bytes.Compare(req, expectReq) != 0 {
					t.Errorf("Expected write of %q", expectReq)
					t.Errorf("          but got %q", req)
				}

				// return canned/expected response from websocket connection
				mockDBResponseChan <- response
				return nil
			},
			readCtxFunc: func(ctx context.Context, msgChan chan message) {
				// limit time for mockDBResponseChan to send us a response
				ctx2, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				select {
				case resp := <-mockDBResponseChan:
					var id string
					var respNotifier interface{}
					cli.responseNotifier.Range(func(key, val interface{}) bool {
						id = key.(string)
						respNotifier = val
						return false
					})

					cli.results.Store(id, []interface{}{resp})
					respNotifier.(chan error) <- nil // inform upstream that final results are available
					cancel()
					return
				case <-ctx2.Done():
					t.Error("Timed out waiting to readCtx chan")
				}
				cancel()
				msgChan <- message{0, nil, errors.New("readCtxFunc timeout")}
			},
			pingCtxFunc:    func(context.Context, chan error) { time.Sleep(5 * time.Second) },
			isDisposedFunc: func() bool { return false },
		}
		cli, err = DialCtx(ctx, dialerMocked, errs)
		return &cli, err
	}
	return NewPool(mockDialFunc)
}

const reqPrefix = `!application/vnd.gremlin-v3.0+json` // length-prefixed string for content-type: 1st byte is len(rest of this string)

type vert struct {
	ID  string `graph:"id,string"`
	Val string `graph:"val,string"`
}
type vert2 struct {
	ID   string   `graph:"id,string"`
	Vals []string `graph:"val,[]string"`
	More int32    `graph:"num,number"`
}

func TestAddV(t *testing.T) {
	type testDataFmt struct {
		// Vert,VertLabel determine the generated request dbReq (so these must match), but neither of these determine dbRes in these tests
		Vert      interface{}
		VertLabel string
		dbReq     []byte
		// dbRes is wrapped graphson, and determines (gets decoded to) ID,Labels,propVals,propMaps
		dbRes    Response
		ID       string
		Labels   []string
		propVals map[string][]string
		propNums map[string][]int32
		propMaps map[string]map[string][]string
	}

	testData := []testDataFmt{
		testDataFmt{
			VertLabel: "laybull",
			Vert:      vert{ID: "eye-dee", Val: "my-val"},
			dbReq: []byte(reqPrefix +
				`{"requestId":"<reqid>","op":"eval","processor":"",` +
				`"args":{"gremlin":"g.addV('laybull').property('id','eye-dee').property('val','my-val')",` +
				`"language":"gremlin-groovy"}}`),
			dbRes: Response{
				RequestID: "ook",
				Status:    Status{Message: "ok", Code: 123},
				Result: Result{
					Meta: nil,
					Data: json.RawMessage(`{"@type":"g:List","@value":[` +
						`{"@type":"g:Vertex","@value":{"id":"test-id","label":"my-label",` +
						`"properties":{` +
						`"health":[` +
						`{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 1},"value":"1212","label":"health"}}` +
						`]}` +
						`}}]}`),
				},
			},
			ID:     "test-id",
			Labels: []string{"my-label"},
			propVals: map[string][]string{
				"health":     {"1212"},
				"not-a-prop": nil,
			},
		},
		testDataFmt{
			VertLabel: "laybull2",
			Vert:      vert2{ID: "eye-dee2", Vals: []string{"my-val1", "my-val2"}, More: 1234},
			dbReq: []byte(reqPrefix +
				`{"requestId":"<reqid>","op":"eval","processor":"",` +
				`"args":{"gremlin":"g.addV('laybull2').property('id','eye-dee2').property('val','my-val1').property('val','my-val2').property('num',1234)",` +
				`"language":"gremlin-groovy"}}`),
			dbRes: Response{
				RequestID: "ook",
				Status:    Status{Message: "ok", Code: 123},
				Result: Result{
					Meta: nil,
					Data: json.RawMessage(`{"@type":"g:List","@value":[` +
						`{"@type":"g:Vertex","@value":{"id":"test-id","label":"my-label",` +
						`"properties":{` +
						`"p2":[` +
						`{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 1},"value":"1212","label":"p2"}},` +
						`{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 2},"value":"3131","label":"p2"}}` +
						`],` +
						`"mapkey":[` +
						`{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 1},"value":"mapkey-val1","label":"mapkey-sub1"}},` +
						`{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 2},"value":"mapkey-val2","label":"mapkey-sub2"}}` +
						`],` +
						`"num":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 1},"value":{"@type":"g:Int32","@value":1234},"label":"num"}}]` +
						`}}}]}`),
				},
			},
			ID:     "test-id",
			Labels: []string{"my-label"},
			propVals: map[string][]string{
				"p2": {"1212", "3131"},
			},
			propNums: map[string][]int32{
				"num": {1234},
			},
			propMaps: map[string]map[string][]string{
				"mapkey": {
					"mapkey-sub1": {"mapkey-val1"},
					"mapkey-sub2": {"mapkey-val2"},
				},
			},
		},
	}

	for _, expect := range testData {
		errs := make(chan error)
		p := MockNewPoolWithDialerCtx(context.Background(), "ws://0", errs, t, expect.dbReq, expect.dbRes)
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		v, err := p.AddVertexCtx(timeoutCtx, expect.VertLabel, expect.Vert)
		if err != nil {
			t.Errorf("Expected err to be nil, but got: %s", err)
		}
		cancel()

		if v.GetID() != expect.ID {
			t.Errorf("Expected id to be %q but got %q", expect.ID, v.GetID())
		}

		labels := v.GetLabels()
		if len(labels) != len(expect.Labels) || strings.Join(labels, "@") != strings.Join(expect.Labels, "@") {
			t.Errorf("Expected labels to be %+v but got %+v", expect.Labels, labels)
		}

		for propKey, expectVals := range expect.propVals {
			gotPropVal, errSingle := v.GetProperty(propKey)
			gotPropVals, errMulti := v.GetMultiProperty(propKey)

			if len(expectVals) == 0 {
				if errSingle == nil {
					t.Errorf("Expected single-val property %q to return ErrorPropertyNotFound but got vals %q", propKey, gotPropVal)
				} else if errSingle != graphson.ErrorPropertyNotFound {
					t.Errorf("Expected single-val property %q to return ErrorPropertyNotFound but got error %q", propKey, errSingle)
				}
				if errMulti == nil {
					t.Errorf("Expected multi-val property %q return ErrorPropertyNotFound but got vals %q", propKey, gotPropVals)
				} else if errMulti != graphson.ErrorPropertyNotFound {
					t.Errorf("Expected multi-val property %q to return ErrorPropertyNotFound but got error %q", propKey, errMulti)
				}

			} else if len(expectVals) > 0 {
				if len(expectVals) == 1 {
					if errSingle != nil {
						t.Errorf("Expected single-val property %q to be %+v but got error %q", propKey, expectVals, errSingle)
					} else if gotPropVal != expectVals[0] {
						t.Errorf("Expected single-val property %q to be %q but got %q", propKey, expectVals[0], gotPropVal)
					}
				} else {
					if errSingle == nil {
						t.Errorf("Expected single-val property %q to return error for expected %+v but got %q", propKey, expectVals, gotPropVal)
					} else if errSingle != graphson.ErrorPropertyIsMulti && errSingle != graphson.ErrorPropertyIsMeta {
						t.Errorf("Expected single-val property %q to return ErrorPropertyIsM* for expected %+v but got error %q", propKey, expectVals, errSingle)
					}
				}

				if errMulti != nil {
					t.Errorf("Expected multi-val property %q to be %+v but got error %q", propKey, expectVals, errMulti)
				} else if len(expectVals) != len(gotPropVals) || strings.Join(expectVals, "@") != strings.Join(gotPropVals, "@") {
					t.Errorf("Expected multi-val property %q to be %+v but got %+v", propKey, expectVals, gotPropVals)
				}
			}
		}

		for propKey, expectNums := range expect.propNums {
			gotPropNum, errSingle := v.GetPropertyInt32(propKey)
			gotPropNums, errMulti := v.GetMultiPropertyInt32(propKey)

			if len(expectNums) == 0 {
				if errSingle == nil {
					t.Errorf("Expected single-val property %q to return ErrorPropertyNotFound but got vals %q", propKey, gotPropNum)
				} else if errSingle != graphson.ErrorPropertyNotFound {
					t.Errorf("Expected single-val property %q to return ErrorPropertyNotFound but got error %q", propKey, errSingle)
				}
				if errMulti == nil {
					t.Errorf("Expected multi-val property %q return ErrorPropertyNotFound but got vals %q", propKey, gotPropNums)
				} else if errMulti != graphson.ErrorPropertyNotFound {
					t.Errorf("Expected multi-val property %q to return ErrorPropertyNotFound but got error %q", propKey, errMulti)
				}

			} else if len(expectNums) > 0 {
				if len(expectNums) == 1 {
					if errSingle != nil {
						t.Errorf("Expected single-val property %q to be %+v but got error %q", propKey, expectNums, errSingle)
					} else if gotPropNum != expectNums[0] {
						t.Errorf("Expected single-val property %q to be %q but got %q", propKey, expectNums[0], gotPropNum)
					}
				} else {
					if errSingle == nil {
						t.Errorf("Expected single-val property %q to return error for expected %+v but got %q", propKey, expectNums, gotPropNum)
					} else if errSingle != graphson.ErrorPropertyIsMulti && errSingle != graphson.ErrorPropertyIsMeta {
						t.Errorf("Expected single-val property %q to return ErrorPropertyIsM* for expected %+v but got error %q", propKey, expectNums, errSingle)
					}
				}

				if errMulti != nil {
					t.Errorf("Expected multi-val property %q to be %+v but got error %q", propKey, expectNums, errMulti)
				} else if len(expectNums) != len(gotPropNums) || fmt.Sprintf("%+v", expectNums) != fmt.Sprintf("%+v", gotPropNums) {
					t.Errorf("Expected multi-val property %q to be %+v but got %+v", propKey, expectNums, gotPropNums)
				}
			}
		}

		for propKey, expectVals := range expect.propMaps {
			gotPropMap, errMap := v.GetMetaProperty(propKey)
			if len(expectVals) == 0 {
				if errMap == nil {
					t.Errorf("Expected multi-val property %q return ErrorPropertyNotFound but got vals %q", propKey, gotPropMap)
				} else if errMap != graphson.ErrorPropertyNotFound {
					t.Errorf("Expected multi-val property %q to return ErrorPropertyNotFound but got error %q", propKey, errMap)
				}
			} else if len(expectVals) > 0 {
				if errMap != nil {
					t.Errorf("Expected multi-val property %q to be %+v but got error %q", propKey, expectVals, errMap)
				} else if len(expectVals) != len(gotPropMap) || fmt.Sprintf("%+v", expectVals) != fmt.Sprintf("%+v", gotPropMap) {
					t.Errorf("Expected multi-val property %q to be %+v but got %+v", propKey, expectVals, gotPropMap)
				}
			}
		}

	}
}

func TestConnectionCleaner(t *testing.T) {
	n := time.Now()

	// invalid has timedout and should be cleaned up
	invalid := &conn{Client: &Client{}, t: n.Add(-1030 * time.Millisecond)}
	// valid has not yet timed out and should remain in the freeConns pool
	valid := &conn{Client: &Client{}, t: n.Add(1030 * time.Millisecond)}

	// Pool has a 30 second timeout and an freeConns connection slice containing both
	// the invalid and valid freeConns connections
	p := NewPool(dummyDialFunc)
	defer p.Close()
	p.MaxLifetime = time.Second * 1
	p.freeConns = []*conn{invalid, valid}

	if len(p.freeConns) != 2 {
		t.Errorf("Expected 2 freeConns connections, got %d", len(p.freeConns))
	}

	p.mu.Lock()
	p.open = len(p.freeConns)
	p.startCleanerLocked()
	p.mu.Unlock()

	time.Sleep(1010 * time.Millisecond)
	if len(p.freeConns) != 1 {
		t.Errorf("Expected 1 freeConns connection after clean, got %d", len(p.freeConns))
		for _, pc := range p.freeConns {
			fmt.Println(pc.t)
		}
	}

	if p.freeConns[0].t != valid.t {
		t.Error("Expected the valid connection to remain in freeConns pool")
	}

}

func TestPurgeErrorClosedConnection(t *testing.T) {
	n := time.Now()

	p := NewPool(dummyDialFunc)
	defer p.Close()
	p.MaxLifetime = time.Second * 1

	valid := &conn{Client: &Client{}, t: n.Add(1030 * time.Millisecond)}

	client := &Client{}

	closed := &conn{Pool: p, Client: client, t: n.Add(1030 * time.Millisecond)}

	freeConns := []*conn{valid, closed}

	p.freeConns = freeConns

	// Simulate error
	closed.Client.Errored = true

	if len(p.freeConns) != 2 {
		t.Errorf("Expected 2 idle connections, got %d", len(p.freeConns))
	}

	p.mu.Lock()
	p.open = len(p.freeConns)
	p.startCleanerLocked()
	p.mu.Unlock()
	time.Sleep(1010 * time.Millisecond)

	if len(p.freeConns) != 1 {
		t.Errorf("Expected 1 freeConns connection after clean, got %d", len(p.freeConns))
	}

	if p.freeConns[0] != valid {
		t.Error("Expected valid connection to remain in pool")
	}
}

func TestPooledConnectionClose(t *testing.T) {
	pool := NewPool(dummyDialFunc)
	defer pool.Close()
	pc := &conn{Pool: pool}

	if len(pool.freeConns) != 0 {
		t.Errorf("Expected 0 freeConns connection, got %d", len(pool.freeConns))
	}

	pool.putConn(pc, nil)

	if len(pool.freeConns) != 1 {
		t.Errorf("Expected 1 freeConns connection, got %d", len(pool.freeConns))
	}

	freeConnsd := pool.freeConns[0]

	if freeConnsd == nil {
		t.Error("Expected to get connection")
	}
}

func TestFirst(t *testing.T) {
	n := time.Now()
	pool := NewPool(dummyDialFunc)
	defer pool.Close()
	pool.MaxOpen = 1
	pool.MaxLifetime = 30 * time.Millisecond
	freeConnsd := []*conn{
		&conn{Pool: pool, Client: &Client{}, t: n.Add(-45 * time.Millisecond)}, // expired
		&conn{Pool: pool, Client: &Client{}, t: n.Add(-45 * time.Millisecond)}, // expired
		&conn{Pool: pool, Client: &Client{}},                                   // valid
	}
	pool.freeConns = freeConnsd

	if len(pool.freeConns) != 3 {
		t.Errorf("Expected 3 freeConns connection, got %d", len(pool.freeConns))
	}

	// Empty pool should return nil
	emptyPool := &Pool{}

	if len(emptyPool.freeConns) != 0 {
		t.Errorf("Expected nil, got %d", len(emptyPool.freeConns))
	}
}

func TestGetAndDial(t *testing.T) {
	n := time.Now()

	client := &Client{}
	pool := NewPool(func() (*Client, error) {
		return client, nil
	})
	defer pool.Close()
	pool.MaxLifetime = time.Millisecond * 30

	invalid := &conn{Pool: pool, Client: &Client{}, t: n.Add(-30 * time.Millisecond)}

	freeConns := []*conn{invalid}
	pool.freeConns = freeConns

	if len(pool.freeConns) != 1 {
		t.Error("Expected 1 freeConns connection")
	}

	if pool.freeConns[0] != invalid {
		t.Error("Expected invalid connection")
	}

	pool.mu.Lock()
	pool.startCleanerLocked()
	pool.mu.Unlock()
	time.Sleep(1010 * time.Millisecond)

	conn, err := pool.conn()

	if err != nil {
		t.Error(err)
	}

	if len(pool.freeConns) != 0 {
		t.Errorf("Expected 0 freeConns connections, got %d", len(pool.freeConns))
	}

	if conn.Client != client {
		t.Error("Expected correct client to be returned")
	}

	if pool.open != 0 {
		t.Errorf("Expected 0 opened connection, got %d", pool.open)
	}

	// Close the connection and ensure it was returned to the freeConns pool
	pool.putConn(conn, nil)

	if len(pool.freeConns) != 1 {
		t.Error("Expected connection to be returned to freeConns pool")
	}

	// Get a new connection and ensure that it is the now idling connection
	conn, err = pool.conn()

	if err != nil {
		t.Error(err)
	}

	if conn.Client != client {
		t.Error("Expected the same connection to be reused")
	}
}
