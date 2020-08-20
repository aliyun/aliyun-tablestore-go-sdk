package search

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/otsprotocol"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSerialize(t *testing.T) {
	scanQuery := scanQuery{
		Query:             &MatchQuery{
			FieldName:          "field1",
			Text:               "value1",
		},
		Limit:             proto.Int32(6),
		AliveTime:         proto.Int32(36),
		Token:             []byte("abc"),
		CurrentParallelID: proto.Int32(1),
		MaxParallel:       proto.Int32(12),
	}
	scanQueryBytes, _ := scanQuery.Serialize()

	//expected
	matchQueryPB := &otsprotocol.MatchQuery{
		FieldName:            proto.String("field1"),
		Text:                 proto.String("value1"),
	}
	matchQueryBytes, _ := proto.Marshal(matchQueryPB)
	queryPB := &otsprotocol.Query{
		Type:                 QueryType_MatchQuery.ToPB(),
		Query:                matchQueryBytes,
	}
	scanQueryPB := &otsprotocol.ScanQuery{
		Query:                queryPB,
		Limit:                proto.Int32(6),
		AliveTime:            proto.Int32(36),
		Token:                []byte("abc"),
		CurrentParallelId:    proto.Int32(1),
		MaxParallel:          proto.Int32(12),
	}

	scanQueryBytesExpected, _ := proto.Marshal(scanQueryPB)
	assert.Equal(t, scanQueryBytesExpected, scanQueryBytes)
}

func TestSerializeLimit(t *testing.T) {
	{	//nil
		scanQuery := scanQuery{}
		scanQueryBytes, _ := scanQuery.Serialize()

		scanQuery2 := &otsprotocol.ScanQuery{}
		proto.Unmarshal(scanQueryBytes, scanQuery2)

		assert.Nil(t, scanQuery2.Limit)
	}
	{	//<0
		scanQuery := scanQuery{
			Limit: proto.Int32(-1),
		}
		scanQueryBytes, _ := scanQuery.Serialize()

		scanQuery2 := &otsprotocol.ScanQuery{}
		proto.Unmarshal(scanQueryBytes, scanQuery2)

		assert.Nil(t, scanQuery2.Limit)
	}
	{	//>=0
		scanQuery := scanQuery{
			Limit: proto.Int32(0),
		}
		scanQueryBytes, _ := scanQuery.Serialize()

		scanQuery2 := &otsprotocol.ScanQuery{}
		proto.Unmarshal(scanQueryBytes, scanQuery2)

		assert.Equal(t, proto.Int32(0), scanQuery2.Limit)
	}
}

func TestSerializeAliveTime(t *testing.T) {
	{	//nil
		scanQuery := scanQuery{}
		scanQueryBytes, _ := scanQuery.Serialize()

		scanQuery2 := &otsprotocol.ScanQuery{}
		proto.Unmarshal(scanQueryBytes, scanQuery2)

		assert.Nil(t, scanQuery2.AliveTime)
	}
	{	//<=0
		scanQuery := scanQuery{
			AliveTime: proto.Int32(0),
		}
		scanQueryBytes, _ := scanQuery.Serialize()

		scanQuery2 := &otsprotocol.ScanQuery{}
		proto.Unmarshal(scanQueryBytes, scanQuery2)

		assert.Nil(t, scanQuery2.AliveTime)
	}
	{	//>0
		scanQuery := scanQuery{
			AliveTime: proto.Int32(1),
		}
		scanQueryBytes, _ := scanQuery.Serialize()

		scanQuery2 := &otsprotocol.ScanQuery{}
		proto.Unmarshal(scanQueryBytes, scanQuery2)

		assert.Equal(t, proto.Int32(1), scanQuery2.AliveTime)
	}
}

func TestSerializeToken(t *testing.T) {
	{	//token == nil
		scanQuery := scanQuery{}
		scanQueryBytes, _ := scanQuery.Serialize()

		scanQuery2 := &otsprotocol.ScanQuery{}
		proto.Unmarshal(scanQueryBytes, scanQuery2)

		assert.Nil(t, scanQuery2.Token)
	}
	{	//len(token) > 0
		scanQuery := scanQuery{
			Token: []byte("xyz"),
		}
		scanQueryBytes, _ := scanQuery.Serialize()

		scanQuery2 := &otsprotocol.ScanQuery{}
		proto.Unmarshal(scanQueryBytes, scanQuery2)

		assert.Equal(t, []byte("xyz"), scanQuery2.Token)
	}
}

func TestSerializeCurrentParallelID(t *testing.T) {
	{	//nil
		scanQuery := scanQuery{}
		scanQueryBytes, _ := scanQuery.Serialize()

		scanQuery2 := &otsprotocol.ScanQuery{}
		proto.Unmarshal(scanQueryBytes, scanQuery2)

		assert.Nil(t, scanQuery2.CurrentParallelId)
	}
	{	//<0
		scanQuery := scanQuery{
			CurrentParallelID: proto.Int32(-1),
		}
		scanQueryBytes, _ := scanQuery.Serialize()

		scanQuery2 := &otsprotocol.ScanQuery{}
		proto.Unmarshal(scanQueryBytes, scanQuery2)

		assert.Nil(t, scanQuery2.CurrentParallelId)
	}
	{	//>=0
		scanQuery := scanQuery{
			CurrentParallelID: proto.Int32(0),
		}
		scanQueryBytes, _ := scanQuery.Serialize()

		scanQuery2 := &otsprotocol.ScanQuery{}
		proto.Unmarshal(scanQueryBytes, scanQuery2)

		assert.Equal(t, proto.Int32(0), scanQuery2.CurrentParallelId)
	}
}

func TestSerializeMaxParallel(t *testing.T) {
	{	//nil
		scanQuery := scanQuery{}
		scanQueryBytes, _ := scanQuery.Serialize()

		scanQuery2 := &otsprotocol.ScanQuery{}
		proto.Unmarshal(scanQueryBytes, scanQuery2)

		assert.Nil(t, scanQuery2.MaxParallel)
	}
	{	//<=0
		scanQuery := scanQuery{
			MaxParallel: proto.Int32(0),
		}
		scanQueryBytes, _ := scanQuery.Serialize()

		scanQuery2 := &otsprotocol.ScanQuery{}
		proto.Unmarshal(scanQueryBytes, scanQuery2)

		assert.Nil(t, scanQuery2.MaxParallel)
	}

	{	//>0
		scanQuery := scanQuery{
			MaxParallel: proto.Int32(1),
		}
		scanQueryBytes, _ := scanQuery.Serialize()

		scanQuery2 := &otsprotocol.ScanQuery{}
		proto.Unmarshal(scanQueryBytes, scanQuery2)

		assert.Equal(t, proto.Int32(1), scanQuery2.MaxParallel)
	}
}