package search

import (
	"github.com/golang/protobuf/proto"
	"github.com/lanjingren/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
)

type WildcardQuery struct {
	FieldName string
	Value     string
}

func (q *WildcardQuery) Type() QueryType {
	return QueryType_WildcardQuery
}

func (q *WildcardQuery) Serialize() ([]byte, error) {
	query := &otsprotocol.WildcardQuery{}
	query.FieldName = &q.FieldName
	query.Value = &q.Value
	data, err := proto.Marshal(query)
	return data, err
}

func (q *WildcardQuery) ProtoBuffer() (*otsprotocol.Query, error) {
	return BuildPBForQuery(q)
}
