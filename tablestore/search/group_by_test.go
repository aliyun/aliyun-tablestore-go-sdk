package search

import (
	"errors"
	"github.com/aliyun/aliyun-tablestore-go-sdk/v5/tablestore/otsprotocol"
	"github.com/stretchr/testify/assert"
	"testing"
)

//mockGroupBy
type mockGroupBy struct {
}

func (g *mockGroupBy) GetName() string {
	return "mock_group_by"
}

func (a *mockGroupBy) GetType() GroupByType {
	return GroupByFilterType
}

func (a *mockGroupBy) Serialize() ([]byte, error) {
	return []byte("hello group by"), nil
}

func (a *mockGroupBy) ProtoBuffer() (*otsprotocol.GroupBy, error) {
	return BuildPBForGroupBy(a)
}

//invalidBodyGroupBy
type invalidBodyGroupBy struct {
}

func (g *invalidBodyGroupBy) GetName() string {
	return "invalid_body_group_by"
}

func (a *invalidBodyGroupBy) GetType() GroupByType {
	return GroupByRangeType
}

func (a *invalidBodyGroupBy) Serialize() ([]byte, error) {
	return nil, errors.New("invalid body")
}

func (a *invalidBodyGroupBy) ProtoBuffer() (*otsprotocol.GroupBy, error) {
	return BuildPBForGroupBy(a)
}

func TestBuildPBForGroupBy(t *testing.T) {
	mockGroupBy := &mockGroupBy{}

	pbMockGroupBy, err := BuildPBForGroupBy(mockGroupBy)
	assert.Nil(t, err)
	assert.Equal(t, "mock_group_by", *pbMockGroupBy.Name)
	assert.Equal(t, otsprotocol.GroupByType_GROUP_BY_FILTER, *pbMockGroupBy.Type)
	assert.Equal(t, []byte("hello group by"), pbMockGroupBy.Body)
}

func TestBuildPBForGroupByInvalidBody(t *testing.T) {
	invalidBodyGroupBy := &invalidBodyGroupBy{}
	_, err := BuildPBForGroupBy(invalidBodyGroupBy)
	assert.Equal(t, "invalid body", err.Error())
}

func TestBuildPBForGroupBys(t *testing.T) {
	mockGroupBy := &mockGroupBy{}
	groupBys := []GroupBy {
		mockGroupBy,
	}
	pbGroupBys, err := BuildPBForGroupBys(groupBys)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(pbGroupBys.GroupBys))

	assert.Equal(t, "mock_group_by", *pbGroupBys.GroupBys[0].Name)
	assert.Equal(t, otsprotocol.GroupByType_GROUP_BY_FILTER, *pbGroupBys.GroupBys[0].Type)
	assert.Equal(t, []byte("hello group by"), pbGroupBys.GroupBys[0].Body)
}

func TestBuildPBForGroupBysInvalid(t *testing.T) {
	mockGroupBy := &mockGroupBy{}
	invalidBodyGroupBy := &invalidBodyGroupBy{}
	groupBys := []GroupBy {
		mockGroupBy,
		invalidBodyGroupBy,
	}
	_, err := BuildPBForGroupBys(groupBys)
	assert.Equal(t, "invalid group by: invalid body", err.Error())
}

func TestBuildPBForGroupBysNilAggs(t *testing.T) {
	pbAggs, err := BuildPBForGroupBys(nil)
	assert.Nil(t, pbAggs)
	assert.Nil(t, err)
}

func TestBuildPBForGroupBysAggs(t *testing.T) {
	pbAggs, err := BuildPBForGroupBys([]GroupBy{})
	assert.Nil(t, pbAggs)
	assert.Nil(t, err)
}
