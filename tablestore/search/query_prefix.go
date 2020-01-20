package search

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/v5/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
)

type PrefixQuery struct {
	FieldName string
	Prefix    string
}

func (q *PrefixQuery) Type() QueryType {
	return QueryType_PrefixQuery
}

func (q *PrefixQuery) Serialize() ([]byte, error) {
	query := &otsprotocol.PrefixQuery{}
	query.FieldName = &q.FieldName
	query.Prefix = &q.Prefix
	data, err := proto.Marshal(query)
	return data, err
}

func (q *PrefixQuery) ProtoBuffer() (*otsprotocol.Query, error) {
	return BuildPBForQuery(q)
}
