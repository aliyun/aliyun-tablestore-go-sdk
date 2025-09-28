package search

import (
	"encoding/json"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
)

type SearchFilter struct {
	Query      Query      `json:"-"`
	QueryAlias queryAlias `json:"Query"`
}

func (f *SearchFilter) SetQuery(query Query) *SearchFilter {
	f.Query = query
	return f
}

func (f *SearchFilter) ProtoBuffer() (*otsprotocol.SearchFilter, error) {
	filter := &otsprotocol.SearchFilter{}
	if f.Query != nil {
		pbQuery, err := f.Query.ProtoBuffer()
		if err != nil {
			return nil, err
		}
		filter.Query = pbQuery
	}
	return filter, nil
}

func (f *SearchFilter) MarshalJSON() (data []byte, err error) {
	type filterAlias SearchFilter
	filter := filterAlias(*f)
	if f.Query != nil {
		filter.QueryAlias = queryAlias{
			Name:  f.Query.Type().String(),
			Query: f.Query,
		}
	}
	data, err = json.Marshal(filter)
	return
}

func (f *SearchFilter) UnmarshalJSON(data []byte) (err error) {
	type filterAlias SearchFilter
	fAlias := &filterAlias{}
	err = json.Unmarshal(data, fAlias)
	if err != nil {
		return
	}
	f.Query = fAlias.QueryAlias.Query
	return
}
