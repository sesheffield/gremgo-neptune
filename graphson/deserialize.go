package graphson

import (
	"bytes"
	"encoding/json"
	"errors"

	gutil "github.com/gedge/gremgo-neptune/utils"
)

func DeserializeVertices(rawResponse string) ([]Vertex, error) {
	// TODO: empty strings for property values will cause invalid json
	// make so it can handle that case
	if len(rawResponse) == 0 {
		return []Vertex{}, nil
	}
	return DeserializeVerticesFromBytes([]byte(rawResponse))
}

func DeserializeVerticesFromBytes(rawResponse []byte) ([]Vertex, error) {
	// TODO: empty strings for property values will cause invalid json
	// make so it can handle that case
	var response []Vertex
	if len(rawResponse) == 0 {
		return response, nil
	}
	dec := json.NewDecoder(bytes.NewReader(rawResponse))
	dec.DisallowUnknownFields()
	err := dec.Decode(&response)
	// panic("urror")
	if err != nil {
		return nil, err
	}
	return response, nil
}

func DeserializeListOfVerticesFromBytes(rawResponse []byte) ([]Vertex, error) {
	var metaResponse ListVertices
	var response []Vertex
	if len(rawResponse) == 0 {
		return response, nil
	}
	dec := json.NewDecoder(bytes.NewReader(rawResponse))
	dec.DisallowUnknownFields()
	err := dec.Decode(&metaResponse)
	if err != nil {
		return nil, err
	}

	if metaResponse.Type != "g:List" {
		gutil.Dump("unlist ", metaResponse)
		return response, errors.New("Expected `g:List` type")
	}

	return metaResponse.Value, nil
}

func DeserializeListOfEdgesFromBytes(rawResponse []byte) (Edges, error) {
	var metaResponse ListEdges
	var response Edges
	if len(rawResponse) == 0 {
		return response, nil
	}
	dec := json.NewDecoder(bytes.NewReader(rawResponse))
	dec.DisallowUnknownFields()
	err := dec.Decode(&metaResponse)
	if err != nil {
		return nil, err
	}

	if metaResponse.Type != "g:List" {
		gutil.Dump("unlist ", metaResponse)
		return response, errors.New("Expected `g:List` type")
	}

	return metaResponse.Value, nil
}

func DeserializeMapFromBytes(rawResponse []byte) (resMap map[string]interface{}, err error) {
	var metaResponse GList
	if len(rawResponse) == 0 {
		return
	}
	dec := json.NewDecoder(bytes.NewReader(rawResponse))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&metaResponse); err != nil {
		return nil, err
	}

	if metaResponse.Type != "g:Map" {
		gutil.Dump("unmap ", metaResponse)
		return resMap, errors.New("Expected `g:Map` type")
	}

	return resMap, nil
}

func DeserializeEdges(rawResponse string) (Edges, error) {
	var response Edges
	if rawResponse == "" {
		return response, nil
	}
	err := json.Unmarshal([]byte(rawResponse), &response)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func DeserializeGenericValue(rawResponse string) (GenericValue, error) {
	var response GenericValue
	if rawResponse == "" {
		return response, nil
	}
	err := json.Unmarshal([]byte(rawResponse), &response)
	if err != nil {
		return response, err
	}
	return response, nil
}

func DeserializeGenericValues(rawResponse string) (GenericValues, error) {
	var response GenericValues
	if rawResponse == "" {
		return response, nil
	}
	err := json.Unmarshal([]byte(rawResponse), &response)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func ConvertToCleanVertices(vertices []Vertex) []CleanVertex {
	var responseVertices []CleanVertex
	for _, vertex := range vertices {
		responseVertices = append(responseVertices, CleanVertex{
			Id:    vertex.Value.ID,
			Label: vertex.Value.Label,
		})
	}
	return responseVertices
}

func ConvertToCleanEdges(edges Edges) []CleanEdge {
	var responseEdges []CleanEdge
	for _, edge := range edges {
		responseEdges = append(responseEdges, CleanEdge{
			Source: edge.Value.InV,
			Target: edge.Value.OutV,
		})
	}
	return responseEdges
}
