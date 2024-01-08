package search

import (
	"encoding/json"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInnerHits_MarshalJSON(t *testing.T) {
	innerHits := &InnerHits{
		Limit: proto.Int32(0),
		Offset: proto.Int32(10),
		Sort: &Sort{
			Sorters: []Sorter{
				&DocSort{
					SortOrder: SortOrder_ASC.Enum(),
				},
			},
		},
		Highlight: &Highlight{
			HighlightEncoder: PlainMode.Enum(),
			FieldHighlightParameters: map[string]*HighlightParameter{
				"col1": {
					HighlightFragmentOrder: TextSequence.Enum(),
					NumberOfFragments: proto.Int32(5),
					FragmentSize: proto.Int32(100),
					PreTag: proto.String("<b>"),
					PostTag: proto.String("</b>"),
				},
			},
		},
	}
	data, err := json.Marshal(innerHits)
	assert.Nil(t, err)
	newInnerHits := &InnerHits{}
	err = json.Unmarshal(data, newInnerHits)
	assert.Nil(t, err)
	assert.Equal(t, *innerHits.Limit, *newInnerHits.Limit)
	assert.Equal(t, *innerHits.Offset, *newInnerHits.Offset)
	assert.Equal(t, len(innerHits.Sort.Sorters), len(newInnerHits.Sort.Sorters))
	assert.Equal(t, innerHits.Highlight.HighlightEncoder.String(), newInnerHits.Highlight.HighlightEncoder.String())
	assert.Equal(t, innerHits.Highlight.FieldHighlightParameters["col1"].HighlightFragmentOrder.String(), innerHits.Highlight.FieldHighlightParameters["col1"].HighlightFragmentOrder.String())
	assert.Equal(t, *innerHits.Highlight.FieldHighlightParameters["col1"].NumberOfFragments, *newInnerHits.Highlight.FieldHighlightParameters["col1"].NumberOfFragments)
	assert.Equal(t, *innerHits.Highlight.FieldHighlightParameters["col1"].FragmentSize, *newInnerHits.Highlight.FieldHighlightParameters["col1"].FragmentSize)
	assert.Equal(t, *innerHits.Highlight.FieldHighlightParameters["col1"].PreTag, *newInnerHits.Highlight.FieldHighlightParameters["col1"].PreTag)
	assert.Equal(t, *innerHits.Highlight.FieldHighlightParameters["col1"].PostTag, *newInnerHits.Highlight.FieldHighlightParameters["col1"].PostTag)
}