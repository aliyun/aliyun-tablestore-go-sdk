package search

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/search/model"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func TestGroupByGeoGrid_GetName(t *testing.T) {
	group := GroupByGeoGrid{}
	group.SetGroupByName("group1")
	assert.Equal(t, "group1", group.GroupByName)
}

func TestGroupByGeoGrid_GetType(t *testing.T) {
	group := GroupByGeoGrid{}
	assert.Equal(t, GroupByGeoGridType, group.GetType())
}

func TestGroupByGeoGrid_GetFieldName(t *testing.T) {
	group := GroupByGeoGrid{}
	group.SetField("col1")
	assert.Equal(t, "col1", group.Field)
}

func TestGroupByGeoGrid_GetField(t *testing.T) {
	group := GroupByGeoGrid{}
	group.SetField("col1")
	assert.Equal(t, "col1", group.GetField())
}

func TestGroupByGeoGrid_GetSize(t *testing.T) {
	group := GroupByGeoGrid{}
	group.SetSize(1)
	assert.Equal(t, int64(1), group.Size)
}

func TestGroupByGeoGrid_GetPrecision(t *testing.T) {
	group := GroupByGeoGrid{}
	group.SetPrecision(model.GHP_156KM_156KM_3)
	assert.Equal(t, model.GHP_156KM_156KM_3, group.Precision)
}

func TestGroupByGeoGrid_SubAggregation(t *testing.T) {
	subAggList := []Aggregation{
		&MaxAggregation{AggName: "sub_agg1", Field: "field1"},
		&SumAggregation{AggName: "sub_agg2", Field: "field2"},
	}

	{
		group := GroupByGeoGrid{}
		group.SubAggregations(subAggList[0]).SubAggregation(subAggList[1])
		assert.Equal(t, subAggList, group.SubAggList)
	}
	{
		group := GroupByGeoGrid{}
		group.SubAggregations(subAggList[0], subAggList[1])
		assert.Equal(t, subAggList, group.SubAggList)
	}
}

func TestGroupByGeoGrid_SubGroupBy(t *testing.T) {
	subGroupByList := []GroupBy{
		&GroupByFilter{AggName: "sub_group_by1", Queries: []Query{&MatchAllQuery{}}},
		&GroupByRange{AggName: "sub_group_by2", Field: "field3", RangeList: []Range{{from: 1, to: 2}, {from: 2, to: math.Inf(1)}}},
	}

	{
		group := GroupByGeoGrid{}
		group.SubGroupBy(subGroupByList[0]).SubGroupBy(subGroupByList[1])
		assert.Equal(t, subGroupByList, group.SubGroupByList)
	}
	{
		group := GroupByGeoGrid{}
		group.SubGroupBys(subGroupByList[0], subGroupByList[1])
		assert.Equal(t, subGroupByList, group.SubGroupByList)
	}
}
