package search


import (
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func TestGroupByFilter_GetName(t *testing.T) {
	group := GroupByFilter{}
	group.Name("group1")
	assert.Equal(t, "group1", group.AggName)
}

func TestGroupByFilter_GetType(t *testing.T) {
	group := GroupByFilter{}
	assert.Equal(t, GroupByFilterType, group.GetType())
}

func TestGroupByFilter_Query(t *testing.T) {
	queries := []Query {
		&MatchAllQuery{},
		&TermQuery{FieldName: "field1", Term: "value"},
	}

	group := GroupByFilter{}
	group.Query(queries[0]).Query(queries[1])

	assert.Equal(t, queries, group.Queries)
}

func TestGroupByFilter_SubAggregation(t *testing.T) {
	subAggList := []Aggregation {
		&MaxAggregation{AggName: "sub_agg1", Field: "field1"},
		&SumAggregation{AggName: "sub_agg2", Field: "field2"},
	}

	{
		group := GroupByFilter{}
		group.SubAggregations(subAggList[0]).SubAggregation(subAggList[1])
		assert.Equal(t, subAggList, group.SubAggList)
	}
	{
		group := GroupByFilter{}
		group.SubAggregations(subAggList[0], subAggList[1])
		assert.Equal(t, subAggList, group.SubAggList)
	}
}

func TestGroupByFilter_SubGroupBy(t *testing.T) {
	subGroupByList := []GroupBy {
		&GroupByFilter{AggName: "sub_group_by1", Queries: []Query {&MatchAllQuery{}}},
		&GroupByRange{AggName: "sub_group_by2", Field: "field3", RangeList: []Range{{from: 1, to: 2}, {from:2, to: math.Inf(1)}}},
	}

	{
		group := GroupByFilter{}
		group.SubGroupBy(subGroupByList[0]).SubGroupBy(subGroupByList[1])
		assert.Equal(t, subGroupByList, group.SubGroupByList)
	}
	{
		group := GroupByFilter{}
		group.SubGroupBys(subGroupByList[0], subGroupByList[1])
		assert.Equal(t, subGroupByList, group.SubGroupByList)
	}
}