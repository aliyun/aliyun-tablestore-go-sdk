package search

import (
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func TestGroupByField_GetName(t *testing.T) {
	group := GroupByField{}
	group.Name("group1")
	assert.Equal(t, "group1", group.AggName)
}

func TestGroupByField_GetType(t *testing.T) {
	group := GroupByField{}
	assert.Equal(t, GroupByFieldType, group.GetType())
}

func TestGroupByField_FieldName(t *testing.T) {
	group := GroupByField{}
	group.FieldName("col1")
	assert.Equal(t, "col1", group.Field)
}

func TestGroupByField_Size(t *testing.T) {
	group := GroupByField{}
	group.Size(6)
	assert.Equal(t, int32(6), *group.Sz)
}

func TestGroupByField_GroupBySorters(t *testing.T) {
	sorters := []GroupBySorter{
		&GroupKeyGroupBySort{
			Order: SortOrder_DESC.Enum(),
		},
		&RowCountGroupBySort{
			Order: SortOrder_ASC.Enum(),
		},
		&SubAggGroupBySort{
			Order:      SortOrder_DESC.Enum(),
			SubAggName: "sub_agg1",
		},
	}

	group := GroupByField{}
	group.GroupBySorters(sorters)
	assert.Equal(t, sorters, group.Sorters)

}

func TestGroupByField_SubAggregation(t *testing.T) {
	subAggList := []Aggregation{
		&MaxAggregation{AggName: "sub_agg1", Field: "field1"},
		&SumAggregation{AggName: "sub_agg2", Field: "field2"},
	}

	{
		group := GroupByField{}
		group.SubAggregations(subAggList[0]).SubAggregation(subAggList[1])
		assert.Equal(t, subAggList, group.SubAggList)
	}
	{
		group := GroupByField{}
		group.SubAggregations(subAggList[0], subAggList[1])
		assert.Equal(t, subAggList, group.SubAggList)
	}
}

func TestGroupByField_SubGroupBy(t *testing.T) {
	subGroupByList := []GroupBy{
		&GroupByFilter{AggName: "sub_group_by1", Queries: []Query{&MatchAllQuery{}}},
		&GroupByRange{AggName: "sub_group_by2", Field: "field3", RangeList: []Range{{from: 1, to: 2}, {from: 2, to: math.Inf(1)}}},
	}

	{
		group := GroupByField{}
		group.SubGroupBy(subGroupByList[0]).SubGroupBy(subGroupByList[1])
		assert.Equal(t, subGroupByList, group.SubGroupByList)
	}
	{
		group := GroupByField{}
		group.SubGroupBys(subGroupByList[0], subGroupByList[1])
		assert.Equal(t, subGroupByList, group.SubGroupByList)
	}
}
