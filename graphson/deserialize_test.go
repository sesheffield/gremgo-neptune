package graphson

import (
	"fmt"
	"testing"
)

func TestDeserializeVertices(t *testing.T) {
	givens := []string{
		// test empty response
		`[]`,
		// test single vertex, single property
		`[{"@type":"g:Vertex","@value":{"id":"test-id","label":"label","properties":{"health":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 1},"value":"1","label":"health"}}]}}}]`,
		// test two vertices, single property
		`[{"@type":"g:Vertex","@value":{"id":"test-id","label":"label","properties":{"health":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 1},"value":"1","label":"health"}}]}}}, {"@type":"g:Vertex","@value":{"id":"test-id2","label":"label","properties":{"health":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 1},"value":"1","label":"health"}}]}}}]`,
		// test single vertex, two properties
		`[{"@type":"g:Vertex","@value":{"id":"test-id","label":"label","properties":{"health":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 1},"value":"1","label":"health"}}], "health2":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 1},"value":"2","label":"health2"}}]}}}]`,
		// test single vertex, single property - but property has multiple values
		`[{"@type":"g:Vertex","@value":{"id":"test-id","label":"label","properties":{"health":[{"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 1},"value":"1","label":"health"}}, {"@type":"g:VertexProperty","@value":{"id":{"@type":"Type","@value": 1},"value":"2","label":"health"}}]}}}]`,
	}
	expecteds := [][]Vertex{
		{},
		{MakeDummyVertex("test-id", "label", map[string]interface{}{"health": 1})},
		{MakeDummyVertex("test-id", "label", map[string]interface{}{"health": 1}), MakeDummyVertex("test-id2", "label", map[string]interface{}{"health": 1})},
		{MakeDummyVertex("test-id", "label", map[string]interface{}{"health": 1, "health2": 2})},
		{MakeDummyVertex("test-id", "label", map[string]interface{}{"health": []interface{}{1, 2}})},
	}
	for i, given := range givens {
		expected := expecteds[i]
		result, err := DeserializeVertices(given)

		if err != nil || len(result) != len(expected) {
			t.Error("given", given, "expected", expected, "result", result, "err", err)
		}
		for j, resultVertex := range result {
			expectedVertex := expected[j]

			if !VerticesMatch(resultVertex, expectedVertex) {
				t.Error("given", given, "expected", expectedVertex.Value.Properties, "result", resultVertex.Value.Properties)
			}
		}
	}
}

func TestDeserializeEdges(t *testing.T) {
	// givens := []string{
	// 	// test empty response
	// 	`[]`,
	// 	// test single edge, single property
	// 	`[{"@type":"g:Edge","@value":{"id":{"@type":"g:Int32", "@value":101},"label":"label","inVLabel":"inVLabel","outVLabel":"outVLabel","inV":{"@type":"g:Int32", "@value": 11},"outV":{"@type":"g:Int32", "@value": 22},"properties":{"test":{"@type":"g:Property","@value":{"key":"test","value":{"@type":"g:Int32", "@value": 3}}}}}}]`,
	// 	// test two edges, single property
	// 	`[	 {"@type":"g:Edge","@value":{"id":{"@type":"g:Int32", "@value":102},"label":"label","inVLabel":"inVLabel","outVLabel":"outVLabel","inV":{"@type":"g:Int32", "@value": 11},"outV":{"@type":"g:Int32", "@value": 22},"properties":{"test":{"@type":"g:Property","@value":{"key":"test","value":{"@type":"g:Int32", "@value": 3}}}}}},` +
	// 		`{"@type":"g:Edge","@value":{"id":{"@type":"g:Int32", "@value":1021},"label":"label","inVLabel":"inVLabel","outVLabel":"outVLabel","inV":{"@type":"g:Int32", "@value": 111},"outV":{"@type":"g:Int32", "@value": 222},"properties":{"test":{"@type":"g:Property","@value":{"key":"test","value":{"@type":"g:Int32", "@value": 31}}}}}}` +
	// 		`]`,
	// 	// test single edge, multiple properties
	// 	`[{"@type":"g:Edge","@value":{"id":{"@type":"g:Int32", "@value":103},"label":"label","inVLabel":"inVLabel","outVLabel":"outVLabel","inV":{"@type":"g:Int32", "@value": 11},"outV":{"@type":"g:Int32", "@value": 22},"properties":{"test":{"@type":"g:Property","@value":{"key":"test","value":{"@type":"g:Int32", "@value": 3}}}, "test2":{"@type":"g:Property","@value":{"key":"test2","value":{"@type":"g:Int32", "@value": 3}}}}}}]`,
	// }
	givens := []string{
		// test empty response
		`[]`,
		// test single edge, single property
		`[{"@type":"g:Edge","@value":{"id":"101","label":"label","inVLabel":"inVLabel","outVLabel":"outVLabel","inV":"11","outV":"22","properties":{"test":{"@type":"g:Property","@value":{"key":"test","value":{"@type":"g:Int32", "@value": 3}}}}}}]`,
		// test two edges, single property
		`[	 {"@type":"g:Edge","@value":{"id":"102","label":"label","inVLabel":"inVLabel","outVLabel":"outVLabel","inV":"11","outV":"22","properties":{"test":{"@type":"g:Property","@value":{"key":"test","value":{"@type":"g:Int32", "@value": 3}}}}}},` +
			`{"@type":"g:Edge","@value":{"id":"1021","label":"label","inVLabel":"inVLabel","outVLabel":"outVLabel","inV":"111","outV":"222","properties":{"test":{"@type":"g:Property","@value":{"key":"test","value":{"@type":"g:Int32", "@value": 31}}}}}}` +
			`]`,
		// test single edge, multiple properties
		`[{"@type":"g:Edge","@value":{"id":"103","label":"label","inVLabel":"inVLabel","outVLabel":"outVLabel","inV":"11","outV":"22","properties":{"test":{"@type":"g:Property","@value":{"key":"test","value":{"@type":"g:Int32", "@value": 3}}}, "test2":{"@type":"g:Property","@value":{"key":"test2","value":{"@type":"g:Int32", "@value": 3}}}}}}]`,
	}
	expecteds := []Edges{
		{},
		{MakeDummyEdge(101, "label", "inVLabel", "outVLabel", "11", "22", map[string]int32{"test": 3})},
		{MakeDummyEdge(102, "label", "inVLabel", "outVLabel", "11", "22", map[string]int32{"test": 3}), MakeDummyEdge(1021, "label", "inVLabel", "outVLabel", "111", "222", map[string]int32{"test": 31})},
		{MakeDummyEdge(103, "label", "inVLabel", "outVLabel", "11", "22", map[string]int32{"test": 3, "test2": 22})},
	}

	for i, given := range givens {
		expected := expecteds[i]
		result, err := DeserializeEdges(given)
		if err != nil {
			t.Error("given", given, "\n\t expected", expected, "\n\t   result", result, "\n\t      err", err)
		}

		if len(result) != len(expected) {
			t.Error("given", given, "\n\t expected", expected, "\n\t   result", result, "\n\t      bad lengths")
		}

		for j, resultEdge := range result {
			expectedEdge := expected[j]
			col := []string{"\x1b[0m", "\x1b[32m", "\x1b[33m"}
			if ok, reason := EdgesMatch(resultEdge, expectedEdge); !ok {
				expectedEdgeString := fmt.Sprintf("%s%+v%s", col[1], expectedEdge, col[0])
				resultEdgeString := fmt.Sprintf("%s%+v%s", col[2], resultEdge, col[0])
				t.Error("given", given, "\n\t expected", expectedEdgeString, "\n\t   result", resultEdgeString, "\n\t  reason", reason)
			}
		}
	}
}

// original was value()
func TestDeserializeGenericValues(t *testing.T) {
	givens := []string{
		// test empty response
		`[]`,
		// test single gv, core return type
		`[{"@type":"generic1", "@value": 1}]`,
		// test 2 gv, core return type
		`[{"@type":"generic2.1", "@value": 21}, {"@type":"generic2.2", "@value": "test"}]`,
		// // test single gv, map return type
		`[{"@type":"generic3", "@value": {"test": "test1"}}]`,
		// // test single gv, nested map return type
		`[{"@type":"generic4", "@value": {"test": {"test": "test"}}}]`,
	}
	expecteds := [][]GenericValue{
		{},
		{MakeDummyGenericValue("generic1", 1)},
		{MakeDummyGenericValue("generic2.1", 21), MakeDummyGenericValue("generic2.2", "test")},
		{MakeDummyGenericValue("generic3", map[string]string{"test": "test1"})},
		{MakeDummyGenericValue("generic4", map[string]interface{}{"test": map[string]string{"test": "test"}})},
	}

	for i, given := range givens {
		expected := expecteds[i]
		result, err := DeserializeGenericValues(given)

		if err != nil || len(result) != len(expected) {
			t.Error("given", given, "expected", expected, "result", result, "err", err)
		}

		for j, resultGenericValue := range result {
			expectedGenericValue := expected[j]
			if ok, reason := GenericValuesMatch(resultGenericValue, expectedGenericValue); !ok {
				t.Error("given", given, "expected", expectedGenericValue, "result", resultGenericValue, "reason", reason)
			}
		}
	}
}

func TestDeserializeGenericValue(t *testing.T) {
	givens := []string{
		// test empty response
		`{}`,
		// test single gv, core return type
		`{"@type":"generic1", "@value": 1}`,
		// // test single gv, map return type
		`{"@type":"generic2", "@value": {"test": "test1"}}`,
		// // test single gv, nested map return type
		`{"@type":"generic3", "@value": {"test": {"test": "test"}}}`,
	}
	expecteds := []GenericValue{
		{},
		MakeDummyGenericValue("generic1", 1),
		MakeDummyGenericValue("generic2", map[string]string{"test": "test1"}),
		MakeDummyGenericValue("generic3", map[string]interface{}{"test": map[string]string{"test": "test"}}),
	}
	// 	givens := []string{
	// 	// test empty response
	// 	`{}`,
	// 	// test single gv, nested map return type
	// 	`{"@type":"g:List", "@value": [{"@type":"g:Edge", "@value": {"XXX 13": {"test": "test"}}}]}`, // XXX
	// 	// 		`{"@type":"g:List", "@value": [
	// 	// {
	// 	//   "@type" : "g:Edge",
	// 	//   "@value" : {
	// 	//     "id" : {
	// 	//       "@type" : "g:Int32",
	// 	//       "@value" : 13
	// 	//     },
	// 	//     "label" : "develops",
	// 	//     "inVLabel" : "software",
	// 	//     "outVLabel" : "person",
	// 	//     "inV" : {
	// 	//       "@type" : "g:Int32",
	// 	//       "@value" : 10
	// 	//     },
	// 	//     "outV" : {
	// 	//       "@type" : "g:Int32",
	// 	//       "@value" : 1
	// 	//     },
	// 	//     "properties" : {
	// 	//       "since" : {
	// 	//         "@type" : "g:Property",
	// 	//         "@value" : {
	// 	//           "key" : "since",
	// 	//           "value" : {
	// 	//             "@type" : "g:Int32",
	// 	//             "@value" : 2009
	// 	//           }
	// 	//         }
	// 	//       }
	// 	//     }
	// 	//   }
	// 	// }
	// 	// ]}`,
	// }
	// expecteds := GenericValues{
	// 	{},
	// 	MakeDummyGenericValue("g:List", MakeDummyGenericValue("g:Edge", MakeDummyGenericValue("testV", "testV2", "testE1", 11, 22, map[string]int32{"prop1": 123}))),
	// }

	for i, given := range givens {
		expected := expecteds[i]
		result, err := DeserializeGenericValue(given)
		if err != nil { // || len(result) != len(expected) {
			t.Error("given", given, "\nexpected", expected, "\nresult", result, "\nerr", err)
		}

		// for j, resultGenericValue := range result {
		// 	expectedGenericValue := expected[j]
		if ok, reason := GenericValuesMatch(result, expected); !ok {
			t.Error("given", given, "\n\t\t  expected", expected, "\n\t\t    result", result, "reason", reason)
		}
		// }
	}
}

func TestConvertToCleanVertices(t *testing.T) {
	givens := [][]Vertex{
		{},
		{MakeDummyVertex("test-id", "label", map[string]interface{}{"health": 1})},
		{MakeDummyVertex("test-id", "label", map[string]interface{}{"health": 1}), MakeDummyVertex("test-id2", "label", map[string]interface{}{"health": 1})},
	}
	expecteds := [][]CleanVertex{
		{},
		{CleanVertex{Id: "test-id", Label: "label"}},
		{CleanVertex{Id: "test-id", Label: "label"}, CleanVertex{Id: "test-id2", Label: "label"}},
	}

	for i, given := range givens {
		expected := expecteds[i]
		result := ConvertToCleanVertices(given)

		if len(result) != len(expected) {
			t.Error("given", given, "expected", expected, "result", result)
		}

		for j, resultCleanVertex := range result {
			expectedCleanVertex := expected[j]
			if expectedCleanVertex.Id != resultCleanVertex.Id || expectedCleanVertex.Label != expectedCleanVertex.Label {
				t.Error("given", given, "expected", expected, "result", result)
			}
		}
	}
}

func TestConvertToCleanEdges(t *testing.T) {
	givens := []Edges{
		{},
		{MakeDummyEdge(10, "label", "inVLabel", "outVLabel", "11", "22", map[string]int32{"test": 1})},
		{
			MakeDummyEdge(10, "label", "inVLabel", "outVLabel", "11", "22", map[string]int32{"test": 2}),
			MakeDummyEdge(101, "label", "inVLabel", "outVLabel", "111", "222", map[string]int32{"test": 2}),
		},
	}
	expecteds := [][]CleanEdge{
		{},
		// {CleanEdge{Source: MakeDummyGenericValue("g:Int32", 11), Target: MakeDummyGenericValue("g:Int32", 22)}},
		// {
		// 	CleanEdge{Source: MakeDummyGenericValue("g:Int32", 11), Target: MakeDummyGenericValue("g:Int32", 22)},
		// 	CleanEdge{Source: MakeDummyGenericValue("g:Int32", 111), Target: MakeDummyGenericValue("g:Int32", 222)},
		// },
		{CleanEdge{Source: "11", Target: "22"}},
		{
			CleanEdge{Source: "11", Target: "22"},
			CleanEdge{Source: "111", Target: "222"},
		},
	}

	for i, given := range givens {
		expected := expecteds[i]
		result := ConvertToCleanEdges(given)

		if len(result) != len(expected) {
			t.Error("given", given, "\n\t expected", expected, "\n\t   result", result, "\n\texpected len", len(expected), "result len", len(result))
		}

		for j, resultCleanEdges := range result {
			expectedCleanEdges := expected[j]
			// if matching, reason := GenericValuesMatch(expectedCleanEdges.Source, resultCleanEdges.Source); !matching {
			if expectedCleanEdges.Source != resultCleanEdges.Source {
				reason := "source"
				t.Error("given", given, "\n\t expected", expected, "\n\t   result", result, "\n\t   reason", reason)
			}
			// if matching, reason := GenericValuesMatch(expectedCleanEdges.Target, resultCleanEdges.Target); !matching {
			if expectedCleanEdges.Target != resultCleanEdges.Target {
				reason := "target"
				t.Error("given", given, "\n\t expected", expected, "\n\t   result", result, "\n\t   reason", reason)
			}
		}
	}
}
