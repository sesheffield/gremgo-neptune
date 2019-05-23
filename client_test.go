package gremgo

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestGremlin(t *testing.T) {
	type testGremlin struct {
		title       string
		label       string
		input       interface{}
		expectAdd   string
		expectGet   string
		expectError error
	}

	type StructSane struct {
		Id         string
		Prop       string `graph:"prop,string"`
		IgnoreProp string
	}
	type StructSaneNoId struct {
		Prop       string `graph:"prop,string"`
		IgnoreProp string
	}
	type StructNoTags struct {
		Prop  string
		Prop2 string
	}
	type StructOtherId struct {
		Idee string `graph:"id,id"`
		Prop string `graph:"prop,string"`
	}
	type StructTypes struct {
		PropBool  bool     `graph:"prop,bool"`
		PropArray []string `graph:"ps,[]string"`
	}

	res := []testGremlin{
		{
			title:     "simple",
			input:     StructSane{Id: "simple-id", Prop: "prop-val", IgnoreProp: "ignore-prop"},
			label:     "laybull",
			expectAdd: "addV('laybull').property(id,'simple-id').property('prop','prop-val')",
			expectGet: "V('laybull').hasId('simple-id').has('prop','prop-val')",
		},
		{
			title:     "escaped prop",
			input:     StructSane{Id: "simple-id", Prop: "prop-o'val", IgnoreProp: "ignore-prop"},
			label:     "escapee",
			expectAdd: `addV('escapee').property(id,'simple-id').property('prop','prop-o\'val')`,
			expectGet: `V('escapee').hasId('simple-id').has('prop','prop-o\'val')`,
		},
		{
			title:     "no-id simple",
			input:     StructSaneNoId{Prop: "prop-val", IgnoreProp: "ignore-prop"},
			label:     "no-eye-dee",
			expectAdd: "addV('no-eye-dee').property('prop','prop-val')",
			expectGet: "V('no-eye-dee').has('prop','prop-val')",
		},
		{
			title:     "no-id escaped",
			input:     StructSaneNoId{Prop: "prop-o'val", IgnoreProp: "ignore-prop"},
			label:     "no-eye-dee-esc",
			expectAdd: `addV('no-eye-dee-esc').property('prop','prop-o\'val')`,
			expectGet: `V('no-eye-dee-esc').has('prop','prop-o\'val')`,
		},
		{
			title:       "no-tags error",
			input:       StructNoTags{Prop: "p", Prop2: "p2"},
			label:       "no-tags",
			expectError: ErrorNoGraphTags,
		},
		{
			title:     "check types",
			input:     StructTypes{PropBool: true, PropArray: []string{"ook", "foo"}},
			label:     "typer",
			expectAdd: `addV('typer').property('prop',true).property('ps','ook').property('ps','foo')`,
			expectGet: `V('typer').has('prop',true).has('ps','ook').has('ps','foo')`,
		},
		{
			title:     "check non-Id ID",
			input:     StructOtherId{Idee: "idee-id", Prop: "prop-val"},
			label:     "other-field",
			expectAdd: `addV('other-field').property(id,'idee-id').property('prop','prop-val')`,
			expectGet: `V('other-field').hasId('idee-id').has('prop','prop-val')`,
		},
	}

	for _, gTest := range res {
		Convey("Test "+gTest.title, t, func() {
			var err error
			var outAdd, outGet string
			if outAdd, outGet, err = GremlinForVertex(gTest.label, gTest.input); err != nil {
				if gTest.expectError == nil {
					So(err, ShouldBeNil)
				}
			} else {
				// err is nil
				if gTest.expectError != nil {
					So(err, ShouldNotBeNil)
				} else {
					So(outAdd, ShouldEqual, gTest.expectAdd)
					So(outGet, ShouldEqual, gTest.expectGet)
				}
			}
		})
	}
}
