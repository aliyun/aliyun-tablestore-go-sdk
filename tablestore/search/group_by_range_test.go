package search

import (
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func TestGroupByRange_GetName(t *testing.T) {
	group := GroupByRange{}
	group.Name("group1")
	assert.Equal(t, "group1", group.AggName)
}

func TestGroupByRange_GetType(t *testing.T) {
	group := GroupByRange{}
	assert.Equal(t, GroupByRangeType, group.GetType())
}

func TestGroupByRange_FieldName(t *testing.T) {
	group := GroupByRange{}
	group.FieldName("col1")
	assert.Equal(t, "col1", group.Field)
}

func TestGroupByRange_Range(t *testing.T) {
	ranges := []Range{
		{from: math.Inf(-1), to: 6.6},
		{from: 6.6, to: 9.9},
		{from: 9.9, to: math.Inf(1)},
	}
	group := GroupByRange{}
	group.Range(ranges[0].from, ranges[0].to)
	group.Range(ranges[1].from, ranges[1].to)
	group.Range(ranges[2].from, ranges[2].to)
	assert.Equal(t, ranges, group.RangeList)
}

func TestGroupByRange_SubAggregation(t *testing.T) {
	subAggList := []Aggregation{
		&MaxAggregation{AggName: "sub_agg1", Field: "field1"},
		&SumAggregation{AggName: "sub_agg2", Field: "field2"},
	}

	{
		group := GroupByRange{}
		group.SubAggregations(subAggList[0]).SubAggregation(subAggList[1])
		assert.Equal(t, subAggList, group.SubAggList)
	}
	{
		group := GroupByRange{}
		group.SubAggregations(subAggList[0], subAggList[1])
		assert.Equal(t, subAggList, group.SubAggList)
	}
}

func TestGroupByRange_SubGroupBy(t *testing.T) {
	subGroupByList := []GroupBy{
		&GroupByFilter{AggName: "sub_group_by1", Queries: []Query{&MatchAllQuery{}}},
		&GroupByRange{AggName: "sub_group_by2", Field: "field3", RangeList: []Range{{from: 1, to: 2}, {from: 2, to: math.Inf(1)}}},
	}

	{
		group := GroupByRange{}
		group.SubGroupBy(subGroupByList[0]).SubGroupBy(subGroupByList[1])
		assert.Equal(t, subGroupByList, group.SubGroupByList)
	}
	{
		group := GroupByRange{}
		group.SubGroupBys(subGroupByList[0], subGroupByList[1])
		assert.Equal(t, subGroupByList, group.SubGroupByList)
	}
}
