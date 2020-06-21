package gremgo

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"testing"
)

func TestStreamRead(t *testing.T) {

	rowContent := "example,row,content,"
	expectedRow := rowContent + "\n"

	cursor := &Cursor{"cursorId"}

	// return a single string response when retrieve is called
	retriever := &RetrieverMock{
		retrieveNextResponseCtxFunc: func(ctx context.Context, cursor *Cursor) (responses []Response, eof bool, err error) {
			responses = []Response{createResponse(rowContent)}
			eof = true
			err = nil
			return responses, eof, err
		},
	}

	s := &Stream{
		cursor: cursor,
		eof:    false,
		buffer: []string{},
		client: retriever,
	}

	// first call to read should return the row
	got, err := s.Read()
	if err != nil {
		t.Errorf("Read() error = %v, wantNilErr", err)
		return
	}
	if got != expectedRow {
		t.Errorf("Read() got = %v, want %v", got, expectedRow)
		return
	}

	// the second call to read should return EOF error
	got, err = s.Read()
	if err != io.EOF {
		t.Errorf("Read() error = %v, want %v", err, io.EOF)
		return
	}
	if got != "" {
		t.Errorf("Read() got = %v, want %v", got, "")
		return
	}
}

func TestStreamRead_MultipleResponsesAtOnce(t *testing.T) {

	rowContent := "example,row,content,"
	cursor := &Cursor{"cursorId"}

	retriever := &RetrieverMock{
		retrieveNextResponseCtxFunc: func(ctx context.Context, cursor *Cursor) (responses []Response, eof bool, err error) {
			responses = []Response{
				createResponse(rowContent + `1`),
				createResponse(rowContent + `2`),
			}
			eof = true
			err = nil
			return responses, eof, err
		},
	}

	s := &Stream{
		cursor: cursor,
		eof:    false,
		buffer: []string{},
		client: retriever,
	}

	// the first call to read should return a row
	expectedRow := rowContent + "1\n"
	got, err := s.Read()
	if err != nil {
		t.Errorf("Read() error = %v, wantNilErr", err)
		return
	}
	if got != expectedRow {
		t.Errorf("Read() got = %v, want %v", got, expectedRow)
		return
	}

	// the second call to read should return a row
	expectedRow = rowContent + "2\n"
	got, err = s.Read()
	if err != nil {
		t.Errorf("Read() error = %v, wantNilErr", err)
		return
	}
	if got != expectedRow {
		t.Errorf("Read() got = %v, want %v", got, expectedRow)
		return
	}

	// the third call to read should return EOF error
	got, err = s.Read()
	if err != io.EOF {
		t.Errorf("Read() error = %v, want %v", err, io.EOF)
		return
	}
	if got != "" {
		t.Errorf("Read() got = %v, want %v", got, "")
		return
	}
}

func TestStreamRead_MultipleResponses(t *testing.T) {

	rowContent := "example,row,content,"
	cursor := &Cursor{"cursorId"}

	retrieveCallCount := 0

	retriever := &RetrieverMock{
		retrieveNextResponseCtxFunc: func(ctx context.Context, cursor *Cursor) (responses []Response, eof bool, err error) {
			retrieveCallCount++

			if retrieveCallCount == 1 {
				responses = []Response{
					createResponse(rowContent + strconv.Itoa(retrieveCallCount)),
				}
				eof = false
				err = nil
				return responses, eof, err
			}

			responses = []Response{
				createResponse(rowContent + strconv.Itoa(retrieveCallCount)),
			}
			eof = true
			err = nil
			return responses, eof, err
		},
	}

	s := &Stream{
		cursor: cursor,
		eof:    false,
		buffer: []string{},
		client: retriever,
	}

	// the first call to read should return a row
	expectedRow := rowContent + "1\n"
	got, err := s.Read()
	if err != nil {
		t.Errorf("Read() error = %v, wantNilErr", err)
		return
	}
	if got != expectedRow {
		t.Errorf("Read() got = %v, want %v", got, expectedRow)
		return
	}

	// the second call to read should return a row
	expectedRow = rowContent + "2\n"
	got, err = s.Read()
	if err != nil {
		t.Errorf("Read() error = %v, wantNilErr", err)
		return
	}
	if got != expectedRow {
		t.Errorf("Read() got = %v, want %v", got, expectedRow)
		return
	}

	// the third call to read should return EOF error
	got, err = s.Read()
	if err != io.EOF {
		t.Errorf("Read() error = %v, want %v", err, io.EOF)
		return
	}
	if got != "" {
		t.Errorf("Read() got = %v, want %v", got, "")
		return
	}
}

func TestStreamRead_EmptyLastResponse(t *testing.T) {

	rowContent := "example,row,content,"
	cursor := &Cursor{"cursorId"}

	retrieveCallCount := 0

	retriever := &RetrieverMock{
		retrieveNextResponseCtxFunc: func(ctx context.Context, cursor *Cursor) (responses []Response, eof bool, err error) {
			retrieveCallCount++

			if retrieveCallCount == 1 {
				responses = []Response{
					createResponse(rowContent + strconv.Itoa(retrieveCallCount)),
				}
				eof = false
				err = nil
				return responses, eof, err
			}

			responses = []Response{}
			eof = true
			err = nil
			return responses, eof, err
		},
	}

	s := &Stream{
		cursor: cursor,
		eof:    false,
		buffer: []string{},
		client: retriever,
	}

	// the first call to read should return a row
	expectedRow := rowContent + "1\n"
	got, err := s.Read()
	if err != nil {
		t.Errorf("Read() error = %v, wantNilErr", err)
		return
	}
	if got != expectedRow {
		t.Errorf("Read() got = %v, want %v", got, expectedRow)
		return
	}

	// the second call to read should return EOF error
	got, err = s.Read()
	if err != io.EOF {
		t.Errorf("Read() error = %v, want %v", err, io.EOF)
		return
	}
	if got != "" {
		t.Errorf("Read() got = %v, want %v", got, "")
		return
	}
}

func TestStreamRead_NoContentResponse(t *testing.T) {

	cursor := &Cursor{"cursorId"}

	retriever := &RetrieverMock{
		retrieveNextResponseCtxFunc: func(ctx context.Context, cursor *Cursor) (responses []Response, eof bool, err error) {
			responses = []Response{
				{
					Status: Status{
						Code: http.StatusNoContent,
					},
				},
			}
			eof = false
			err = nil
			return responses, eof, err
		},
	}

	s := &Stream{
		cursor: cursor,
		eof:    false,
		buffer: []string{},
		client: retriever,
	}

	// the call to read should return EOF error
	got, err := s.Read()
	if err != io.EOF {
		t.Errorf("Read() error = %v, want %v", err, io.EOF)
		return
	}
	if got != "" {
		t.Errorf("Read() got = %v, want %v", got, "")
		return
	}
}

func createResponse(rowContent string) Response {
	return Response{
		RequestID: "",
		Status:    Status{},
		Result: Result{
			Data: json.RawMessage(`{"@type":"g:List","@value":["` + rowContent + `"]}`),
		},
	}
}
