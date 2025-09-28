package search

import (
    "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
    "github.com/golang/protobuf/proto"
    "github.com/stretchr/testify/assert"
    "testing"
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
    assert.Equal(t, knnQuery.MinScore, query.MinScore)
    assert.Equal(t, knnQuery.NumCandidates, query.NumCandidates)
}