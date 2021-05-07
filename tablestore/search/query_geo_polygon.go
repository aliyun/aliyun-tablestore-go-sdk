package search

import (
	"github.com/golang/protobuf/proto"
	"github.com/lanjingren/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
)

type GeoPolygonQuery struct {
	FieldName string
	Points    []string
}

func (q *GeoPolygonQuery) Type() QueryType {
	return QueryType_GeoPolygonQuery
}

func (q *GeoPolygonQuery) Serialize() ([]byte, error) {
	query := &otsprotocol.GeoPolygonQuery{}
	query.FieldName = &q.FieldName
	query.Points = q.Points
	data, err := proto.Marshal(query)
	return data, err
}

func (q *GeoPolygonQuery) ProtoBuffer() (*otsprotocol.Query, error) {
	return BuildPBForQuery(q)
}
