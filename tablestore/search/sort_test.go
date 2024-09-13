package search

import (
	"encoding/json"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSort_MarshalJSON_UnMarshalJSON(t *testing.T) {
	{
		sort := &Sort{
			Sorters: []Sorter{
				&PrimaryKeySort{
					Order: SortOrder_ASC.Enum(),
				},
			},
		}
		if marshal, err := sort.MarshalJSON(); err != nil {
			t.Fatal(err)
		} else {
			newSort := &Sort{}
			if err = json.Unmarshal(marshal, newSort); err != nil {
				t.Fatal(err)
			}
			assert.NotNil(t, newSort)
			assert.Nil(t, newSort.DisableDefaultPkSorter)
			assert.Equal(t, 1, len(newSort.Sorters))
		}
	}
	{
		sort := &Sort{
			Sorters: []Sorter{
				&PrimaryKeySort{
					Order: SortOrder_ASC.Enum(),
				},
			},
			DisableDefaultPkSorter: proto.Bool(false),
		}
		if marshal, err := sort.MarshalJSON(); err != nil {
			t.Fatal(err)
		} else {
			newSort := &Sort{}
			if err = json.Unmarshal(marshal, newSort); err != nil {
				t.Fatal(err)
			}
			assert.NotNil(t, newSort)
			assert.NotNil(t, newSort.DisableDefaultPkSorter)
			assert.False(t, *newSort.DisableDefaultPkSorter)
			assert.Equal(t, 1, len(newSort.Sorters))
		}
	}
	{
		sort := &Sort{
			Sorters: []Sorter{
				&PrimaryKeySort{
					Order: SortOrder_ASC.Enum(),
				},
			},
			DisableDefaultPkSorter: proto.Bool(true),
		}
		if marshal, err := sort.MarshalJSON(); err != nil {
			t.Fatal(err)
		} else {
			newSort := &Sort{}
			if err = json.Unmarshal(marshal, newSort); err != nil {
				t.Fatal(err)
			}
			assert.NotNil(t, newSort)
			assert.NotNil(t, newSort.DisableDefaultPkSorter)
			assert.True(t, *newSort.DisableDefaultPkSorter)
			assert.Equal(t, 1, len(newSort.Sorters))
		}
	}
}

func TestSort_ProtoBuffer(t *testing.T) {
	{
		sort := &Sort{
			Sorters: make([]Sorter, 0),
		}
		pbSort, err := sort.ProtoBuffer()
		assert.Nil(t, err)
		assert.Nil(t, pbSort.DisableDefaultPkSorter)
	}
	{
		sort := &Sort{
			Sorters: make([]Sorter, 0),
			DisableDefaultPkSorter: proto.Bool(true),
		}
		pbSort, err := sort.ProtoBuffer()
		assert.Nil(t, err)
		assert.NotNil(t, pbSort.DisableDefaultPkSorter)
		assert.True(t, pbSort.GetDisableDefaultPkSorter())
	}
	{
		sort := &Sort{
			Sorters: make([]Sorter, 0),
			DisableDefaultPkSorter: proto.Bool(false),
		}
		pbSort, err := sort.ProtoBuffer()
		assert.Nil(t, err)
		assert.NotNil(t, pbSort.DisableDefaultPkSorter)
		assert.False(t, pbSort.GetDisableDefaultPkSorter())
	}
}
