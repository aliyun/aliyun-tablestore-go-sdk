package search

import (
	"testing"

	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
)

func Float32Ptr(f float32) *float32 {
	return &f
}

func TestKnnVectorQuery_Type(t *testing.T) {
	knnVectorQuery := KnnVectorQuery{}
	assert.Equal(t, QueryType_KnnVectorQuery, knnVectorQuery.Type())
}

func TestKnnVectorQuery_Serialize(t *testing.T) {
	fieldName := "test_field"
	knnQuery := &KnnVectorQuery{
		FieldName:          fieldName,
		TopK:               Int32Ptr(5),
		Float32QueryVector: []float32{1.0, 2.0, 3.0},
		Weight:             Float32Ptr(1.5),
		MinScore:           Float32Ptr(0.5),
		NumCandidates:      Int32Ptr(10),
	}
	expected, err := knnQuery.Serialize()
	assert.Nil(t, err)
	query := &otsprotocol.KnnVectorQuery{}
	err = proto.Unmarshal(expected, query)
	assert.Nil(t, err)

	assert.Equal(t, knnQuery.FieldName, *query.FieldName)
	assert.Equal(t, knnQuery.TopK, query.TopK)
	assert.Equal(t, knnQuery.Float32QueryVector, query.Float32QueryVector)
	assert.Equal(t, knnQuery.Weight, query.Weight)
	assert.Equal(t, knnQuery.MinScore, query.MinScore)
	assert.Equal(t, knnQuery.NumCandidates, query.NumCandidates)
}

func TestKnnVectorQuery_Serialize_WithWeight(t *testing.T) {
	// Test with Weight only
	knnQuery := &KnnVectorQuery{
		FieldName:          "vector_field",
		TopK:               Int32Ptr(10),
		Float32QueryVector: []float32{0.1, 0.2, 0.3, 0.4},
		Weight:             Float32Ptr(2.0),
	}
	data, err := knnQuery.Serialize()
	assert.Nil(t, err)
	assert.NotNil(t, data)

	query := &otsprotocol.KnnVectorQuery{}
	err = proto.Unmarshal(data, query)
	assert.Nil(t, err)

	assert.Equal(t, "vector_field", *query.FieldName)
	assert.Equal(t, int32(10), *query.TopK)
	assert.Equal(t, []float32{0.1, 0.2, 0.3, 0.4}, query.Float32QueryVector)
	assert.Equal(t, float32(2.0), *query.Weight)
	assert.Nil(t, query.MinScore)
	assert.Nil(t, query.NumCandidates)
}

func TestKnnVectorQuery_Serialize_WithoutWeight(t *testing.T) {
	// Test without Weight
	knnQuery := &KnnVectorQuery{
		FieldName:          "vector_field",
		TopK:               Int32Ptr(5),
		Float32QueryVector: []float32{1.0, 2.0},
	}
	data, err := knnQuery.Serialize()
	assert.Nil(t, err)
	assert.NotNil(t, data)

	query := &otsprotocol.KnnVectorQuery{}
	err = proto.Unmarshal(data, query)
	assert.Nil(t, err)

	assert.Equal(t, "vector_field", *query.FieldName)
	assert.Equal(t, int32(5), *query.TopK)
	assert.Equal(t, []float32{1.0, 2.0}, query.Float32QueryVector)
	assert.Nil(t, query.Weight)
}
