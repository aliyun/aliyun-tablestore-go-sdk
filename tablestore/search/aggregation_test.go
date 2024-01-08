package search

import (
	"errors"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/stretchr/testify/assert"
	"testing"
)

//mockAggregation
type mockAggregation struct {
}

func (a *mockAggregation) GetName() string {
	return "mock_agg"
}

func (a *mockAggregation) GetType() AggregationType {
	return AggregationAvgType
}

func (a *mockAggregation) Serialize() ([]byte, error) {
	return []byte("hello aggregation"), nil
}

func (a *mockAggregation) ProtoBuffer() (*otsprotocol.Aggregation, error) {
	return BuildPBForAggregation(a)
}

//invalidBodyAggregation
type invalidBodyAggregation struct {
}

func (a *invalidBodyAggregation) GetName() string {
	return "invalid_body_agg"
}

func (a *invalidBodyAggregation) GetType() AggregationType {
	return AggregationMaxType
}

func (a *invalidBodyAggregation) Serialize() ([]byte, error) {
	return nil, errors.New("invalid body")
}

func (a *invalidBodyAggregation) ProtoBuffer() (*otsprotocol.Aggregation, error) {
	return BuildPBForAggregation(a)
}

func TestBuildPBForAggregation(t *testing.T) {
	mockAgg := &mockAggregation{}

	pbMockAgg, err := BuildPBForAggregation(mockAgg)
	assert.Nil(t, err)
	assert.Equal(t, "mock_agg", *pbMockAgg.Name)
	assert.Equal(t, otsprotocol.AggregationType_AGG_AVG, *pbMockAgg.Type)
	assert.Equal(t, []byte("hello aggregation"), pbMockAgg.Body)
}

func TestBuildPBForAggregationInvalidBody(t *testing.T) {
	invalidBodyAgg := &invalidBodyAggregation{}
	_, err := BuildPBForAggregation(invalidBodyAgg)
	assert.Equal(t, "invalid body", err.Error())
}

func TestBuildPBForAggregations(t *testing.T) {
	mockAgg := &mockAggregation{}
	aggs := []Aggregation{
		mockAgg,
	}
	pbAggs, err := BuildPBForAggregations(aggs)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(pbAggs.Aggs))

	assert.Equal(t, "mock_agg", *pbAggs.Aggs[0].Name)
	assert.Equal(t, otsprotocol.AggregationType_AGG_AVG, *pbAggs.Aggs[0].Type)
	assert.Equal(t, []byte("hello aggregation"), pbAggs.Aggs[0].Body)
}

func TestBuildPBForAggregationsInvalid(t *testing.T) {
	mockAgg := &mockAggregation{}
	invalidBodyAgg := &invalidBodyAggregation{}
	aggs := []Aggregation{
		mockAgg,
		invalidBodyAgg,
	}
	_, err := BuildPBForAggregations(aggs)
	assert.Equal(t, "invalid agg: invalid body", err.Error())
}

func TestBuildPBForAggregationsNilAggs(t *testing.T) {
	pbAggs, err := BuildPBForAggregations(nil)
	assert.Nil(t, pbAggs)
	assert.Nil(t, err)
}

func TestBuildPBForAggregationsEmptyAggs(t *testing.T) {
	pbAggs, err := BuildPBForAggregations([]Aggregation{})
	assert.Nil(t, pbAggs)
	assert.Nil(t, err)
}
