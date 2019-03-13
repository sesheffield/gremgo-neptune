package gremgo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	gutil "github.com/gedge/gremgo-neptune/utils"
)

const (
	statusSuccess                  = 200
	statusNoContent                = 204
	statusPartialContent           = 206
	statusUnauthorized             = 401
	statusAuthenticate             = 407
	statusMalformedRequest         = 498
	statusInvalidRequestArguments  = 499
	statusServerError              = 500
	statusScriptEvaluationError    = 597
	statusServerTimeout            = 598
	statusServerSerializationError = 599
)

// Status struct is used to hold properties returned from requests to the gremlin server
type Status struct {
	Message    string                 `json:"message"`
	Code       int                    `json:"code"`
	Attributes map[string]interface{} `json:"attributes"`
}

// Result struct is used to hold properties returned for results from requests to the gremlin server
type Result struct {
	// Query Response Data
	Data json.RawMessage        `json:"data"`
	Meta map[string]interface{} `json:"meta"`
}

// Response structs holds the entire response from requests to the gremlin server
type Response struct {
	RequestID string `json:"requestId"`
	Status    Status `json:"status"`
	Result    Result `json:"result"`
}

// ToString returns a string representation of the Response struct
func (r Response) ToString() string {
	return fmt.Sprintf("Response \nRequestID: %v, \nStatus: {%#v}, \nResult: {%#v}\n", r.RequestID, r.Status, r.Result)
}

func (c *Client) handleResponse(msg []byte) (err error) {
	var resp Response
	resp, err = marshalResponse(msg)
	// err = errors.New("ook")
	if resp.Status.Code == statusAuthenticate { //Server request authentication
		return c.authenticate(resp.RequestID)
	}

	c.saveResponse(resp, err)
	return
}

// marshalResponse creates a response struct for every incoming response for further manipulation
func marshalResponse(msg []byte) (resp Response, err error) {
	err = json.Unmarshal(msg, &resp)
	if err != nil {
		return
	}

	err = resp.detectError()
	return
}

// saveResponse makes the response available for retrieval by the requester. Mutexes are used for thread safety.
func (c *Client) saveResponse(resp Response, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var container []interface{}
	existingData, ok := c.results.Load(resp.RequestID) // Retrieve old data container (for requests with multiple responses)
	if ok {
		container = existingData.([]interface{})
		existingData = nil
		gutil.Dump("more for requestID: %s len: %d data: %v", resp.RequestID, len(resp.Result.Data), resp.Result.Data)
	}
	newdata := append(container, resp)       // Create new data container with new data
	c.results.Store(resp.RequestID, newdata) // Add new data to buffer for future retrieval
	respNotifier, loaded := c.responseNotifier.LoadOrStore(resp.RequestID, make(chan error, 1))
	if !loaded {
		gutil.WarnLev(1, "respNotifier NOT LOADED %s", resp.RequestID)
	}
	// err is from marshalResponse (json.Unmarshal), but is ignored when Code==statusPartialContent
	if resp.Status.Code == statusPartialContent {
		if chunkNotifier, ok := c.chunkNotifier.Load(resp.RequestID); ok {
			gutil.Warn("%s chunk %s", time.Now(), resp.RequestID[:3])
			chunkNotifier.(chan bool) <- true
		}
	} else {
		if err != nil {
			gutil.Warn("%s response DONE: %s", time.Now(), err.Error())
		} else {
			gutil.Warn("%s response DONE", time.Now())
		}
		respNotifier.(chan error) <- err
	}
}

// retrieveResponse retrieves the response saved by saveResponse.
func (c *Client) retrieveResponse(id string) (data []Response, err error) {
	resp, _ := c.responseNotifier.Load(id)
	if err = <-resp.(chan error); err == nil {
		data = c.getCurrentResults(id)
		c.cleanResults(id, resp.(chan error), nil)
	}
	return
}

func (c *Client) getCurrentResults(id string) (data []Response) {
	dataI, ok := c.results.Load(id)
	if !ok {
		return
	}
	d := dataI.([]interface{})
	dataI = nil
	data = make([]Response, len(d))
	if len(d) == 0 {
		return
	}
	for i := range d {
		data[i] = d[i].(Response)
	}
	return
}

func (c *Client) cleanResults(id string, respNotifier chan error, chunkNotifier chan bool) {
	if respNotifier == nil {
		return
	}
	c.responseNotifier.Delete(id)
	gutil.WarnLev(1, "responseNotifier DELETED %s", id)
	close(respNotifier)
	if chunkNotifier != nil {
		close(chunkNotifier)
		c.chunkNotifier.Delete(id)
	}
	c.deleteResponse(id)
}

// retrieveResponseCtx retrieves the response saved by saveResponse.
func (c *Client) retrieveResponseCtx(ctx context.Context, id string) (data []Response, err error) {
	respNotifier, _ := c.responseNotifier.Load(id)
	select {
	case err = <-respNotifier.(chan error):
		if err != nil {
			return
		}
		data = c.getCurrentResults(id)
		c.cleanResults(id, respNotifier.(chan error), nil)
	case <-ctx.Done():
		err = ctx.Err()
	}
	return
}

// retrieveNextResponseCtx retrieves the current response saved by saveResponse, `done` is true when the results are complete (eof)
func (c *Client) retrieveNextResponseCtx(ctx context.Context, id string) (data []Response, done bool, err error) {
	respNotifier, _ := c.responseNotifier.Load(id)
	if respNotifier == nil {
		gutil.WarnLev(1, "retrieveNextResponseCtx got NIL respNotifier - panic? %s", id)
		data = c.getCurrentResults(id)
		c.deleteResponse(id)
		//done = true // XXX check this
		return
	}

	var chunkNotifier chan bool
	if chunkNotifierInterface, ok := c.chunkNotifier.Load(id); ok {
		chunkNotifier = chunkNotifierInterface.(chan bool)
	}

	select {
	case err = <-respNotifier.(chan error):
		if err != nil {
			return
		}
		data = c.getCurrentResults(id)
		c.cleanResults(id, respNotifier.(chan error), chunkNotifier)
		done = true
	case <-chunkNotifier:
		c.mu.Lock()
		data = c.getCurrentResults(id)
		c.deleteResponse(id)
		c.mu.Unlock()
	case <-ctx.Done():
		err = ctx.Err()
	}
	return
}

// deleteResponse deletes the response from the container. Used for cleanup purposes by requester.
func (c *Client) deleteResponse(id string) {
	c.results.Delete(id)
	gutil.WarnLev(1, "results DELETED %s", id[:3])
	return
}

// detectError detects any possible errors in responses from Gremlin Server and generates an error for each code
func (r *Response) detectError() (err error) {
	switch r.Status.Code {
	case statusSuccess, statusNoContent, statusPartialContent:
		break
	case statusUnauthorized:
		err = fmt.Errorf("UNAUTHORIZED - Response Message: %s", r.Status.Message)
	case statusAuthenticate:
		err = fmt.Errorf("AUTHENTICATE - Response Message: %s", r.Status.Message)
	case statusMalformedRequest:
		err = fmt.Errorf("MALFORMED REQUEST - Response Message: %s", r.Status.Message)
	case statusInvalidRequestArguments:
		err = fmt.Errorf("INVALID REQUEST ARGUMENTS - Response Message: %s", r.Status.Message)
	case statusServerError:
		err = fmt.Errorf("SERVER ERROR - Response Message: %s", r.Status.Message)
	case statusScriptEvaluationError:
		err = fmt.Errorf("SCRIPT EVALUATION ERROR - Response Message: %s", r.Status.Message)
	case statusServerTimeout:
		err = fmt.Errorf("SERVER TIMEOUT - Response Message: %s", r.Status.Message)
	case statusServerSerializationError:
		err = fmt.Errorf("SERVER SERIALIZATION ERROR - Response Message: %s", r.Status.Message)
	default:
		err = fmt.Errorf("UNKNOWN ERROR - Response Message: %s", r.Status.Message)
	}
	return
}
