package search

import "github.com/lanjingren/aliyun-tablestore-go-sdk/tablestore/otsprotocol"

type Collapse struct {
	FieldName string
}

func (c *Collapse) ProtoBuffer() (*otsprotocol.Collapse, error) {
	pb := &otsprotocol.Collapse{
		FieldName: &c.FieldName,
	}
	return pb, nil
}
