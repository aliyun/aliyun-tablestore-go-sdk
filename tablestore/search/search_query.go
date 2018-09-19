package search

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
)

type SearchQuery interface {
	Serialize() ([]byte, error)
}

type searchQuery struct {
	Offset      int32
	Limit       int32
	SearchAfter *SearchAfter
	Query       Query
	Collapse    *Collapse
	Sort        *Sort
}

func NewSearchQuery() *searchQuery {
	return &searchQuery{
		Offset: -1,
		Limit: -1,
	}
}

func (s *searchQuery) SetOffset(offset int32) *searchQuery {
	s.Offset = offset
	return s
}

func (s *searchQuery) SetLimit(limit int32) *searchQuery {
	s.Limit = limit
	return s
}

func (s *searchQuery) SetSearchAfter(searchAfter *SearchAfter) *searchQuery {
	s.SearchAfter = searchAfter
	return s
}

func (s *searchQuery) SetQuery(query Query) *searchQuery {
	s.Query = query
	return s
}

func (s *searchQuery) SetCollapse(collapse *Collapse) *searchQuery {
	s.Collapse = collapse
	return s
}

func (s *searchQuery) SetSort(sort *Sort) *searchQuery {
	s.Sort = sort
	return s
}

func (s *searchQuery) Serialize() ([]byte, error) {
	search_query := &otsprotocol.SearchQuery{}
	if s.Offset >= 0 {
		search_query.Offset = &s.Offset
	}
	if s.Limit >= 0 {
		search_query.Limit = &s.Limit
	}
	if s.SearchAfter != nil {
		pbSearchAfter, err := s.SearchAfter.ProtoBuffer()
		if err != nil {
			return nil, err
		}
		search_query.SearchAfter = pbSearchAfter
	}
	if s.Query != nil {
		pbQuery, err := s.Query.ProtoBuffer()
		if err != nil {
			return nil, err
		}
		search_query.Query = pbQuery
	}
	if s.Collapse != nil {
		pbCollapse, err := s.Collapse.ProtoBuffer()
		if err != nil {
			return nil, err
		}
		search_query.Collapse = pbCollapse
	}
	if s.Sort != nil {
		pbSort, err := s.Sort.ProtoBuffer()
		if err != nil {
			return nil, err
		}
		search_query.Sort = pbSort
	}
	data, err := proto.Marshal(search_query)
	return data, err
}
