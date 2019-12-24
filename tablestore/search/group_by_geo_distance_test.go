package search

import (
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func TestGroupByGeoDistance_GetName(t *testing.T) {
	group := GroupByGeoDistance{}
	group.Name("group1")
	assert.Equal(t, "group1", group.AggName)
}

func TestGroupByGeoDistance_GetType(t *testing.T) {
	group := GroupByGeoDistance{}
	assert.Equal(t, GroupByGeoDistanceType, group.GetType())
}

func TestGroupByGeoDistance_FieldName(t *testing.T) {
	group := GroupByGeoDistance{}
	group.FieldName("col1")
	assert.Equal(t, "col1", group.Field)
}

func TestGroupByGeoDistance_CenterPoint(t *testing.T) {
	group := GroupByGeoDistance{}
	group.CenterPoint(30.55, 120.66)
	assert.Equal(t, 30.55, group.Origin.Lat)
	assert.Equal(t, 120.66, group.Origin.Lon)
}

func TestGroupByGeoDistance_Range(t *testing.T) {
	ranges := []Range {
		{from: math.Inf(-1), to: 6.6},
		{from: 6.6, to: 9.9},
		{from: 9.9, to: math.Inf(1)},
	}
	group := GroupByGeoDistance{}
	group.Range(ranges[0].from, ranges[0].to)
	group.Range(ranges[1].from, ranges[1].to)
	group.Range(ranges[2].from, ranges[2].to)
	assert.Equal(t, ranges, group.RangeList)
}

func TestGroupByGeoDistance_SubAggregation(t *testing.T) {
	subAggList := []Aggregation {
		&MaxAggregation{AggName: "sub_agg1", Field: "field1"},
		&SumAggregation{AggName: "sub_agg2", Field: "field2"},
	}

	{
		group := GroupByGeoDistance{}
		group.SubAggregations(subAggList[0]).SubAggregation(subAggList[1])
		assert.Equal(t, subAggList, group.SubAggList)
	}
	{
		group := GroupByGeoDistance{}
		group.SubAggregations(subAggList[0], subAggList[1])
		assert.Equal(t, subAggList, group.SubAggList)
	}
}

func TestGroupByGeoDistance_SubGroupBy(t *testing.T) {
	subGroupByList := []GroupBy {
		&GroupByFilter{AggName: "sub_group_by1", Queries: []Query {&MatchAllQuery{}}},
		&GroupByRange{AggName: "sub_group_by2", Field: "field3", RangeList: []Range{{from: 1, to: 2}, {from:2, to: math.Inf(1)}}},
	}

	{
		group := GroupByGeoDistance{}
		group.SubGroupBy(subGroupByList[0]).SubGroupBy(subGroupByList[1])
		assert.Equal(t, subGroupByList, group.SubGroupByList)
	}
	{
		group := GroupByGeoDistance{}
		group.SubGroupBys(subGroupByList[0], subGroupByList[1])
		assert.Equal(t, subGroupByList, group.SubGroupByList)
	}
}