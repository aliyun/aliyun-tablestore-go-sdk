package search

import (
	"errors"

	"github.com/golang/protobuf/proto"
	"github.com/lanjingren/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
)

type FieldValueFactor struct {
	FieldName string
}

func (f *FieldValueFactor) ProtoBuffer() (*otsprotocol.FieldValueFactor, error) {
	pb := &otsprotocol.FieldValueFactor{}
	pb.FieldName = &f.FieldName
	return pb, nil
}

type FunctionScoreQuery struct {
	Query            Query
	FieldValueFactor *FieldValueFactor
}

func (q *FunctionScoreQuery) Type() QueryType {
	return QueryType_FunctionScoreQuery
}

func (q *FunctionScoreQuery) Serialize() ([]byte, error) {
	if q.Query == nil || q.FieldValueFactor == nil {
		return nil, errors.New("FunctionScoreQuery: Query or FieldValueFactor is nil")
	}
	query := &otsprotocol.FunctionScoreQuery{}
	pbQ, err := q.Query.ProtoBuffer()
	if err != nil {
		return nil, err
	}
	query.Query = pbQ
	pbF, err := q.FieldValueFactor.ProtoBuffer()
	if err != nil {
		return nil, err
	}
	query.FieldValueFactor = pbF
	data, err := proto.Marshal(query)
	return data, err
}

func (q *FunctionScoreQuery) ProtoBuffer() (*otsprotocol.Query, error) {
	return BuildPBForQuery(q)
}
