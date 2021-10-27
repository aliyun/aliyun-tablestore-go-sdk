package search

import (
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func TestGroupByHistogram_GetName(t *testing.T) {
	group := GroupByHistogram{}
	group.SetName("group1")
	assert.Equal(t, "group1", group.GroupByName)
}

func TestGroupByHistogram_GetType(t *testing.T) {
	group := GroupByHistogram{}
	assert.Equal(t, GroupByHistogramType, group.GetType())
}

func TestGroupByHistogram_FieldName(t *testing.T) {
	group := GroupByHistogram{}
	group.SetFieldName("col1")
	assert.Equal(t, "col1", group.Field)
}

func TestGroupByHistogram_GetField(t *testing.T) {
	group := GroupByHistogram{}
	group.SetFieldName("col1")
	assert.Equal(t, "col1", group.GetField())
}

func TestGroupByHistogram_SetMinDocCount(t *testing.T) {
	group := GroupByHistogram{}
	group.SetMinDocCount(1)
	assert.Equal(t, int64(1), *group.MinDocCount)
}

func TestGroupByHistogram_SubAggregation(t *testing.T) {
	subAggList := []Aggregation{
		&MaxAggregation{AggName: "sub_agg1", Field: "field1"},
		&SumAggregation{AggName: "sub_agg2", Field: "field2"},
	}

	{
		group := GroupByHistogram{}
		group.SubAggregations(subAggList[0]).SubAggregation(subAggList[1])
		assert.Equal(t, subAggList, group.SubAggList)
	}
	{
		group := GroupByHistogram{}
		group.SubAggregations(subAggList[0], subAggList[1])
		assert.Equal(t, subAggList, group.SubAggList)
	}
}

func TestGroupByHistogram_SubGroupBy(t *testing.T) {
	subGroupByList := []GroupBy{
		&GroupByFilter{AggName: "sub_group_by1", Queries: []Query{&MatchAllQuery{}}},
		&GroupByRange{AggName: "sub_group_by2", Field: "field3", RangeList: []Range{{from: 1, to: 2}, {from: 2, to: math.Inf(1)}}},
	}

	{
		group := GroupByHistogram{}
		group.SubGroupBy(subGroupByList[0]).SubGroupBy(subGroupByList[1])
		assert.Equal(t, subGroupByList, group.SubGroupByList)
	}
	{
		group := GroupByHistogram{}
		group.SubGroupBys(subGroupByList[0], subGroupByList[1])
		assert.Equal(t, subGroupByList, group.SubGroupByList)
	}
}
