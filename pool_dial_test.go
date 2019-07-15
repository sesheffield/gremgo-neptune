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

const reqPrefix = `!application/vnd.gremlin-v3.0+json` // length-prefixed string for content-type: 1st byte is len(rest of this string)

type StaggeredResponse struct {
	after    time.Duration
	response Response
}

type vert struct {
	ID  string `graph:"id,string"`
	Val string `graph:"val,string"`
}
type vert2 struct {
	ID   string   `graph:"id,string"`
	Vals []string `graph:"val,[]string"`
	More int32    `graph:"num,number"`
}
type ResultVert struct {
	ID       string
	Labels   []string
	propVals map[string][]string
	propNums map[string][]int32
	propMaps map[string]map[string][]string
}
type ResultMeta struct {
	verts []ResultVert
	err   error
}

func MockNewPoolWithDialerCtx(ctx context.Context, dbURL string, errs chan error, t *testing.T, expectReq []byte, expectMeta []StaggeredResponse) (*Pool, *dialerMock) {
	mockDBResponseChan := make(chan StaggeredResponse, 10)
	var cli Client
	dialerMocked := &dialerMock{
		connectCtxFunc: func(context.Context) error { return nil },
		writeFunc: func(req []byte) error {
			// obtain first request-id from client keys (should be only one)
			var id string
			cli.responseNotifier.Range(func(key, val interface{}) bool {
				id = key.(string)
				return false
			})

			if expectReq != nil {
				// replace requestId value in req with (generic) "<reqid>" to facilitate comparison with expected
				req = bytes.Replace(req, []byte(`"requestId":"`+id+`"`), []byte(`"requestId":"<reqid>"`), 1)

				if len(req) != len(expectReq) || bytes.Compare(req, expectReq) != 0 {
					t.Errorf("Expected write of %q", expectReq)
					t.Errorf("          but got %q", req)
				}
			}

			// now that we have intercepted the above query 'en route' to the DB (but was eaten here),
			// we send the faked DB response(s) via the channel to readCtxFunc below (thus faking a read from the websocket)
			for idx, responseMeta := range expectMeta {
				if idx > 0 && responseMeta.after > time.Duration(0) {
					// staggered to allow retrieveNextResponseCtx to consume each responseMeta separately
					time.Sleep(responseMeta.after)
				}
				responseMeta.response.RequestID = id
				mockDBResponseChan <- responseMeta
			}
			return nil
		},
		readCtxFunc: func(ctx context.Context, msgChan chan message) {
			// limit time for mockDBResponseChan to send us a response
			ctx2, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			for reading := true; reading; {
				select {
				case resp := <-mockDBResponseChan:
					var msgBytes []byte
					var err error
					if msgBytes, err = json.Marshal(resp.response); err != nil {
						t.Errorf(err.Error())
						continue
					}
					// send response to saveWorkerCtx
					msgChan <- message{
						msg: msgBytes,
						err: nil,
					}
				case <-ctx2.Done():
					t.Error("Timed out waiting to readCtx chan")
					reading = false
				case <-ctx.Done():
					t.Error("readCtx: ctx done")
					return
				}
			}
			msgChan <- message{0, nil, errors.New("readCtxFunc timeout")}
		},
		pingCtxFunc:    func(context.Context, chan error) { time.Sleep(5 * time.Second) },
		IsDisposedFunc: func() bool { return false },
	}
	mockDialFunc := func() (*Client, error) {
		var err error
		cli, err = DialCtx(ctx, dialerMocked, errs)
		return &cli, err
	}
	go func(errs chan error) {
		for {
			select {
			case <-errs:
				// deal with errors centrally
				// XXX perhaps test that errors appear here, too
			}
		}
	}(errs)
	return NewPool(mockDialFunc), dialerMocked
}

func compareVertices(prefix string, res []graphson.Vertex, expectMeta []ResultVert, t *testing.T) {
	for idx, v := range res {
		if idx+1 > len(expectMeta) {
			t.Errorf(prefix+"Expected number of expectMeta (%d) is less than the number received (%d)", len(expectMeta), len(res))
			break
		}
		expectV := expectMeta[idx]

		if v.GetID() != expectV.ID {
			t.Errorf(prefix+"Expected id to be %q but got %q", expectV.ID, v.GetID())
		}

		labels := v.GetLabels()
		if len(labels) != len(expectV.Labels) || strings.Join(labels, "@") != strings.Join(expectV.Labels, "@") {
			t.Errorf(prefix+"Expected labels to be %+v but got %+v", expectV.Labels, labels)
		}

		for propKey, expectVals := range expectV.propVals {
			gotPropVal, errSingle := v.GetProperty(propKey)
			gotPropVals, errMulti := v.GetMultiProperty(propKey)

			if len(expectVals) == 0 {
				if errSingle == nil {
					t.Errorf(prefix+"Expected single-val property %q to return ErrorPropertyNotFound but got vals %q", propKey, gotPropVal)
				} else if errSingle != graphson.ErrorPropertyNotFound {
					t.Errorf(prefix+"Expected single-val property %q to return ErrorPropertyNotFound but got error %q", propKey, errSingle)
				}
				if errMulti == nil {
					t.Errorf(prefix+"Expected multi-val property %q return ErrorPropertyNotFound but got vals %q", propKey, gotPropVals)
				} else if errMulti != graphson.ErrorPropertyNotFound {
					t.Errorf(prefix+"Expected multi-val property %q to return ErrorPropertyNotFound but got error %q", propKey, errMulti)
				}

			} else if len(expectVals) > 0 {
				if len(expectVals) == 1 {
					if errSingle != nil {
						t.Errorf(prefix+"Expected single-val property %q to be %+v but got error %q", propKey, expectVals, errSingle)
					} else if gotPropVal != expectVals[0] {
						t.Errorf(prefix+"Expected single-val property %q to be %q but got %q", propKey, expectVals[0], gotPropVal)
					}
				} else {
					if errSingle == nil {
						t.Errorf(prefix+"Expected single-val property %q to return error for expected %+v but got %q", propKey, expectVals, gotPropVal)
					} else if errSingle != graphson.ErrorPropertyIsMulti && errSingle != graphson.ErrorPropertyIsMeta {
						t.Errorf(prefix+"Expected single-val property %q to return ErrorPropertyIsM* for expected %+v but got error %q", propKey, expectVals, errSingle)
					}
				}

				if errMulti != nil {
					t.Errorf(prefix+"Expected multi-val property %q to be %+v but got error %q", propKey, expectVals, errMulti)
				} else if len(expectVals) != len(gotPropVals) || strings.Join(expectVals, "@") != strings.Join(gotPropVals, "@") {
					t.Errorf(prefix+"Expected multi-val property %q to be %+v but got %+v", propKey, expectVals, gotPropVals)
				}
			}
		}

		for propKey, expectNums := range expectV.propNums {
			gotPropNum, errSingle := v.GetPropertyInt32(propKey)
			gotPropNums, errMulti := v.GetMultiPropertyInt32(propKey)

			if len(expectNums) == 0 {
				if errSingle == nil {
					t.Errorf(prefix+"Expected single-val property %q to return ErrorPropertyNotFound but got vals %q", propKey, gotPropNum)
				} else if errSingle != graphson.ErrorPropertyNotFound {
					t.Errorf(prefix+"Expected single-val property %q to return ErrorPropertyNotFound but got error %q", propKey, errSingle)
				}
				if errMulti == nil {
					t.Errorf(prefix+"Expected multi-val property %q return ErrorPropertyNotFound but got vals %q", propKey, gotPropNums)
				} else if errMulti != graphson.ErrorPropertyNotFound {
					t.Errorf(prefix+"Expected multi-val property %q to return ErrorPropertyNotFound but got error %q", propKey, errMulti)
				}

			} else if len(expectNums) > 0 {
				if len(expectNums) == 1 {
					if errSingle != nil {
						t.Errorf(prefix+"Expected single-val property %q to be %+v but got error %q", propKey, expectNums, errSingle)
					} else if gotPropNum != expectNums[0] {
						t.Errorf(prefix+"Expected single-val property %q to be %q but got %q", propKey, expectNums[0], gotPropNum)
					}
				} else {
					if errSingle == nil {
						t.Errorf(prefix+"Expected single-val property %q to return error for expected %+v but got %q", propKey, expectNums, gotPropNum)
					} else if errSingle != graphson.ErrorPropertyIsMulti && errSingle != graphson.ErrorPropertyIsMeta {
						t.Errorf(prefix+"Expected single-val property %q to return ErrorPropertyIsM* for expected %+v but got error %q", propKey, expectNums, errSingle)
					}
				}

				if errMulti != nil {
					t.Errorf(prefix+"Expected multi-val property %q to be %+v but got error %q", propKey, expectNums, errMulti)
				} else if len(expectNums) != len(gotPropNums) || fmt.Sprintf("%+v", expectNums) != fmt.Sprintf("%+v", gotPropNums) {
					t.Errorf(prefix+"Expected multi-val property %q to be %+v but got %+v", propKey, expectNums, gotPropNums)
				}
			}
		}

		for propKey, expectVals := range expectV.propMaps {
			gotPropMap, errMap := v.GetMetaProperty(propKey)
			if len(expectVals) == 0 {
				if errMap == nil {
					t.Errorf(prefix+"Expected multi-val property %q return ErrorPropertyNotFound but got vals %q", propKey, gotPropMap)
				} else if errMap != graphson.ErrorPropertyNotFound {
					t.Errorf(prefix+"Expected multi-val property %q to return ErrorPropertyNotFound but got error %q", propKey, errMap)
				}
			} else if len(expectVals) > 0 {
				if errMap != nil {
					t.Errorf(prefix+"Expected multi-val property %q to be %+v but got error %q", propKey, expectVals, errMap)
				} else if len(expectVals) != len(gotPropMap) || fmt.Sprintf("%+v", expectVals) != fmt.Sprintf("%+v", gotPropMap) {
					t.Errorf(prefix+"Expected multi-val property %q to be %+v but got %+v", propKey, expectVals, gotPropMap)
				}
			}
		}
	}
}

func TestVert(t *testing.T) {
	type testDataFmt struct {
		callType string // "AddV" or "Get"
		// for callType="AddV":
		// vert,vertLabel determine the generated request expectRawDBReq (so these must match), but neither of these determine wsResponses in these tests
		vert           interface{}
		vertLabel      string
		expectRawDBReq []byte
		// wsResponses are wrapped graphson faked from websocket, and determine (get decoded to) expectMeta
		wsResponses []StaggeredResponse
		expectMeta  ResultMeta
	}

	testData := []testDataFmt{
		{
			callType:  "AddV",
			vertLabel: "testFail",
			vert:      vert{ID: "eye-dee", Val: "my-val"},
			expectRawDBReq: []byte(reqPrefix +
				`{"requestId":"<reqid>","op":"eval","processor":"",` +
				`"args":{"gremlin":"g.addV('testFail').property('id','eye-dee').property('val','my-val')",` +
				`"language":"gremlin-groovy"}}`),
			wsResponses: []StaggeredResponse{
				{
					response: Response{
						RequestID: "ook",
						Status:    Status{Message: "BOOM", Code: 500},
						// Result: Result{
						// 	Meta: nil,
						// 	// Data: json.RawMessage(``),
						// },
					},
				},
			},
			expectMeta: ResultMeta{
				err: errors.New("SERVER ERROR - Response Message: BOOM"),
			},
		},
		{
			callType:  "AddV",
			vertLabel: "testSimpleVert",
			vert:      vert{ID: "eye-dee", Val: "my-val"},
			expectRawDBReq: []byte(reqPrefix +
				`{"requestId":"<reqid>","op":"eval","processor":"",` +
				`"args":{"gremlin":"g.addV('testSimpleVert').property('id','eye-dee').property('val','my-val')",` +
				`"language":"gremlin-groovy"}}`),
			wsResponses: []StaggeredResponse{
				{
					response: Response{
						RequestID: "ook",
						Status:    Status{Message: "ok", Code: 200},
						Result: Result{
							Meta: nil,
							Data: json.RawMessage(`{"@type":"g:List","@value":[` +
								`{"@type":"g:Vertex","@value":{"id":"test-id","label":"testSimpleVert",` +
								`"properties":{` +
								`"health":[` +
								`{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 1},"value":"1212","label":"health"}}` +
								`]}` +
								`}}]}`),
						},
					},
				},
			},
			expectMeta: ResultMeta{
				verts: []ResultVert{
					{
						ID:     "test-id",
						Labels: []string{"testSimpleVert"},
						propVals: map[string][]string{
							"health":     {"1212"},
							"not-a-prop": nil,
						},
					},
				},
			},
		},
		{
			callType:  "AddV",
			vertLabel: "testVertMeta",
			vert:      vert2{ID: "eye-dee2", Vals: []string{"my-val1", "my-val2"}, More: 1234},
			expectRawDBReq: []byte(reqPrefix +
				`{"requestId":"<reqid>","op":"eval","processor":"",` +
				`"args":{"gremlin":"g.addV('testVertMeta').property('id','eye-dee2').property('val','my-val1').property('val','my-val2').property('num',1234)",` +
				`"language":"gremlin-groovy"}}`),
			wsResponses: []StaggeredResponse{
				{
					response: Response{
						RequestID: "ook",
						Status:    Status{Message: "ok", Code: 200},
						Result: Result{
							Meta: nil,
							Data: json.RawMessage(`{"@type":"g:List","@value":[` +
								`{"@type":"g:Vertex","@value":{"id":"test-id","label":"testVertMeta",` +
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
				},
			},
			expectMeta: ResultMeta{
				verts: []ResultVert{
					{
						ID:     "test-id",
						Labels: []string{"testVertMeta"},
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
				},
			},
		},
		{
			vertLabel: "vertGetTwoResponsesAsOne",
			callType:  "Get",
			wsResponses: []StaggeredResponse{
				{
					response: Response{
						RequestID: "ook",
						Status:    Status{Message: "ok", Code: statusPartialContent},
						Result: Result{
							Meta: nil,
							Data: json.RawMessage(`{"@type":"g:List","@value":[` +
								`{"@type":"g:Vertex","@value":{"id":"test-id1","label":"vertGetTwoResponses",` +
								`"properties":{` +
								`"health":[` +
								`{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 1},"value":"1212","label":"health"}}` +
								`]}` +
								`}}]}`),
						},
					},
				},
				{
					after: 50 * time.Millisecond,
					response: Response{
						RequestID: "ook",
						Status:    Status{Message: "ok", Code: 200},
						Result: Result{
							Meta: nil,
							Data: json.RawMessage(`{"@type":"g:List","@value":[` +
								`{"@type":"g:Vertex","@value":{"id":"test-id2","label":"vertGetTwoResponses",` +
								`"properties":{` +
								`"health":[` +
								`{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 1},"value":"2222","label":"health"}}` +
								`]}}},` +
								`{"@type":"g:Vertex","@value":{"id":"test-id3","label":"vertGetTwoResponses",` +
								`"properties":{` +
								`"p3":[` +
								`{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 1},"value":"3333","label":"p3"}}` +
								`]}}}` +
								`]}`),
						},
					},
				},
			},
			expectMeta: ResultMeta{
				verts: []ResultVert{
					{
						ID:     "test-id1",
						Labels: []string{"vertGetTwoResponses"},
						propVals: map[string][]string{
							"health": {"1212"},
						},
					},
					{
						ID:     "test-id2",
						Labels: []string{"vertGetTwoResponses"},
						propVals: map[string][]string{
							"health": {"2222"},
						},
					},
					{
						ID:     "test-id3",
						Labels: []string{"vertGetTwoResponses"},
						propVals: map[string][]string{
							"p3": {"3333"},
						},
					},
				},
			},
		},
	}

	for testIdx, expect := range testData {
		testPrefix := fmt.Sprintf("Test[%d]<%s>: ", testIdx, expect.vertLabel)

		errs := make(chan error)
		p, dialMock := MockNewPoolWithDialerCtx(context.Background(), "ws://0", errs, t, expect.expectRawDBReq, expect.wsResponses)
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		var v []graphson.Vertex
		var err error
		if expect.callType == "AddV" {
			v = make([]graphson.Vertex, 1)
			v[0], err = p.AddVertexCtx(timeoutCtx, expect.vertLabel, expect.vert, nil, nil)
		} else if expect.callType == "Get" {
			var resp []graphson.Vertex
			resp, err = p.GetCtx(timeoutCtx, "g.V()", nil, nil)
			if err == nil {
				for _, respN := range resp {
					v = append(v, respN)
				}
			}
		} else {
			t.Fatalf("unexpected call type %q", expect.callType)
		}
		cancel()
		if err != nil {
			if expect.expectMeta.err != nil {
				if strings.Index(err.Error(), expect.expectMeta.err.Error()) == -1 {
					t.Errorf(testPrefix+"Expected err to be %q, but got: %s", expect.expectMeta.err, err)
				}
			} else {
				t.Errorf(testPrefix+"Expected err to be nil, but got: %s", err)
			}
			continue
		}
		if expect.expectMeta.err != nil {
			t.Errorf(testPrefix+"Expected err to be %q, but got %v", expect.expectMeta.err, err)
			continue
		}

		if len(dialMock.writeCalls()) != 1 {
			t.Errorf(testPrefix+"Expected number of calls to write() (%d) != 1 (expected)", len(dialMock.writeCalls()))
		}
		if len(dialMock.readCtxCalls()) != 1 {
			t.Errorf(testPrefix+"Expected number of calls to readCtx() (%d) != 1 (expected)", len(dialMock.readCtxCalls()))
		}

		compareVertices(testPrefix, v, expect.expectMeta.verts, t)
	}
}

func TestCursor(t *testing.T) {

	type testDataFmt struct {
		testLabel string
		// wsResponses are wrapped graphson responses, and each determines (gets decoded to) their respective expectMeta
		wsResponses []StaggeredResponse
		expectMeta  []ResultMeta
	}

	testData := []testDataFmt{
		{
			testLabel: "testCursorOneToOne",
			wsResponses: []StaggeredResponse{
				{
					response: Response{
						RequestID: "ook",
						Status:    Status{Message: "ok", Code: 200},
						Result: Result{
							Meta: nil,
							Data: json.RawMessage(`{"@type":"g:List","@value":[` +
								`{"@type":"g:Vertex","@value":{"id":"test-id","label":"my-label",` +
								`"properties":{` +
								`"p2":[` +
								`{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 1},"value":"1212","label":"p2"}},` +
								`{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 2},"value":"3131","label":"p2"}}` +
								`],` +
								`"num":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 1},"value":{"@type":"g:Int32","@value":1234},"label":"num"}}]` +
								`}}}]}`),
						},
					},
				},
			},
			expectMeta: []ResultMeta{
				{
					verts: []ResultVert{
						{
							ID:     "test-id",
							Labels: []string{"my-label"},
							propVals: map[string][]string{
								"p2": {"1212", "3131"},
							},
							propNums: map[string][]int32{
								"num": {1234},
							},
						},
					},
				},
			},
		},
		{
			testLabel: "testCursorOneToTwo",
			wsResponses: []StaggeredResponse{
				{
					response: Response{
						RequestID: "ook",
						Status:    Status{Message: "ok", Code: statusPartialContent},
						Result: Result{
							Meta: nil,
							Data: json.RawMessage(`{"@type":"g:List","@value":[` +
								`{"@type":"g:Vertex","@value":{"id":"test-id1","label":"my-label",` +
								`"properties":{` +
								`"health":[` +
								`{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 1},"value":"1212","label":"health"}}` +
								`]}` +
								`}}]}`),
						},
					},
				},
				{
					after: 50 * time.Millisecond,
					response: Response{
						RequestID: "ook",
						Status:    Status{Message: "ok", Code: 200},
						Result: Result{
							Meta: nil,
							Data: json.RawMessage(`{"@type":"g:List","@value":[` +
								`{"@type":"g:Vertex","@value":{"id":"test-id2","label":"my-label",` +
								`"properties":{` +
								`"health":[` +
								`{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 1},"value":"2222","label":"health"}}` +
								`]}}},` +
								`{"@type":"g:Vertex","@value":{"id":"test-id3","label":"my-label3",` +
								`"properties":{` +
								`"p3":[` +
								`{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 1},"value":"3333","label":"p3"}}` +
								`]}}}` +
								`]}`),
						},
					},
				},
			},
			expectMeta: []ResultMeta{
				{
					verts: []ResultVert{
						{
							ID:     "test-id1",
							Labels: []string{"my-label"},
							propVals: map[string][]string{
								"health": {"1212"},
							},
						},
					},
				},
				{
					verts: []ResultVert{
						{
							ID:     "test-id2",
							Labels: []string{"my-label"},
							propVals: map[string][]string{
								"health": {"2222"},
							},
						},
						{
							ID:     "test-id3",
							Labels: []string{"my-label3"},
							propVals: map[string][]string{
								"p3": {"3333"},
							},
						},
					},
				},
			},
		},
		{
			testLabel: "testCursorFailAtStart",
			wsResponses: []StaggeredResponse{
				{
					response: Response{
						RequestID: "ook",
						Status:    Status{Message: "BANG", Code: 500},
						Result: Result{
							Meta: nil,
							Data: json.RawMessage(`{"@type":"g:List","@value":[` +
								`{"@type":"g:Vertex","@value":{"id":"test-id","label":"my-label",` +
								`"properties":{` +
								`"p2":[` +
								`{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 1},"value":"1212","label":"p2"}},` +
								`{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 2},"value":"3131","label":"p2"}}` +
								`],` +
								`"num":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 1},"value":{"@type":"g:Int32","@value":1234},"label":"num"}}]` +
								`}}}]}`),
						},
					},
				},
			},
			expectMeta: []ResultMeta{
				{
					err: errors.New("SERVER ERROR - Response Message: BANG"),
				},
			},
		},
		{
			testLabel: "testCursorFailAfterResults",
			wsResponses: []StaggeredResponse{
				{
					response: Response{
						RequestID: "ook",
						Status:    Status{Message: "OK", Code: statusPartialContent},
						Result: Result{
							Meta: nil,
							Data: json.RawMessage(`{"@type":"g:List","@value":[` +
								`{"@type":"g:Vertex","@value":{"id":"test-id","label":"my-label",` +
								`"properties":{` +
								`"p2":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 1},"value":"1212","label":"p2"}}` +
								`]` +
								`}}}]}`),
						},
					},
				},
				{
					after: time.Duration(50 * time.Millisecond),
					response: Response{
						RequestID: "ook",
						Status:    Status{Message: "SPLAT", Code: 500},
						Result: Result{
							Meta: nil,
						},
					},
				},
			},
			expectMeta: []ResultMeta{
				{
					verts: []ResultVert{
						{
							ID:     "test-id",
							Labels: []string{"my-label"},
							propVals: map[string][]string{
								"p2": {"1212"},
							},
						},
					},
				},
				{
					err: errors.New("SERVER ERROR - Response Message: SPLAT"),
				},
			},
		},
	}

	for _, expect := range testData {
		testPrefix := fmt.Sprintf("Test<%s>: ", expect.testLabel)
		errs := make(chan error)
		p, dialMock := MockNewPoolWithDialerCtx(context.Background(), "ws://0", errs, t, nil, expect.wsResponses)
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		cursor, err := p.OpenCursorCtx(timeoutCtx, "g.V()", nil, nil)
		if err != nil {
			t.Errorf(testPrefix+"Expected OpenCursorCtx err to be nil, but got: %s", err)
		}
		cancel()

		expectedTotalVertices := 0
		for _, exVerts := range expect.expectMeta {
			expectedTotalVertices += len(exVerts.verts)
		}

		totalSeenVertices := 0
		var eof bool
		for idx := 0; !eof; {
			var res []graphson.Vertex

			timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			res, eof, err = p.ReadCursorCtx(timeoutCtx, cursor)
			cancel()
			if err != nil {
				if expect.expectMeta[idx].err != nil {
					if strings.Index(err.Error(), expect.expectMeta[idx].err.Error()) == -1 {
						t.Errorf(testPrefix+"Expected err to be %q, but got: %s", expect.expectMeta[idx].err, err)
					}
					break
				} else {
					t.Errorf(testPrefix+"Expected ReadCursorCtx err to be nil, but got: %s", err)
					break
				}
			}
			if expect.expectMeta[idx].err != nil {
				t.Errorf(testPrefix+"Expected err to be %q, but got nil", expect.expectMeta[idx].err)
				continue
			}

			if len(res) > 0 {
				totalSeenVertices += len(res)

				compareVertices(testPrefix, res, expect.expectMeta[idx].verts, t)
			}
			idx++
			if !eof && idx >= len(expect.expectMeta) {
				t.Errorf(testPrefix+"Exceeded maximum expected cursor reads %d - giving up", idx+1)
				break
			}
		}
		if totalSeenVertices != expectedTotalVertices {
			t.Errorf(testPrefix+"Expected total number of vertices received (%d) != %d (actual)", expectedTotalVertices, totalSeenVertices)
		}
		if len(dialMock.writeCalls()) != 1 {
			t.Errorf(testPrefix+"Expected number of calls to write() (1) != %d (actual)", len(dialMock.writeCalls()))
		}
		if len(dialMock.readCtxCalls()) != 1 {
			t.Errorf(testPrefix+"Expected number of calls to readCtx() (1) != %d (actual)", len(dialMock.readCtxCalls()))
		}
	}
}
