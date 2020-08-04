package timeline

import (
	"strings"

	"github.com/lanjingren/aliyun-tablestore-go-sdk/tablestore"
)

var DefaultStreamAdapter = &StreamMessageAdapter{
	IdKey:        "Id",
	ContentKey:   "Content",
	TimestampKey: "Timestamp",
	AttrPrefix:   "Attr_",
}

type Message interface{}

func LoadColumnMap(attrs []*tablestore.AttributeColumn) *ColumnMap {
	cols := make(map[string]interface{})
	for _, attr := range attrs {
		cols[attr.ColumnName] = attr.Value
	}
	return FromMap(cols)
}

type MessageAdapter interface {
	Marshal(msg Message) (*ColumnMap, error)
	Unmarshal(cols *ColumnMap) (Message, error)
}

type StreamMessage struct {
	Id        string
	Content   interface{}
	Timestamp int64
	Attr      map[string]interface{}
}

type StreamMessageAdapter struct {
	IdKey        string
	ContentKey   string
	TimestampKey string
	AttrPrefix   string
}

func (s *StreamMessageAdapter) Marshal(msg Message) (*ColumnMap, error) {
	sMsg, ok := msg.(*StreamMessage)
	if !ok {
		return nil, ErrUnexpected
	}

	var cols = NewColumnMap()
	cols.AddStringColumn(s.IdKey, sMsg.Id)
	cols.AddAnyColumn(s.ContentKey, sMsg.Content)
	cols.AddInt64Column(s.TimestampKey, sMsg.Timestamp)
	for key, val := range sMsg.Attr {
		cols.AddAnyColumn(key, val)
	}
	return cols, nil
}

func (s *StreamMessageAdapter) Unmarshal(cols *ColumnMap) (Message, error) {
	sMsg := new(StreamMessage)
	sMsg.Attr = make(map[string]interface{})
	for key, val := range cols.ToMap() {
		switch key {
		case s.IdKey:
			sMsg.Id = val.(string)
		case s.ContentKey:
			sMsg.Content = val
		case s.TimestampKey:
			sMsg.Timestamp = val.(int64)
		default:
			if strings.HasPrefix(key, s.AttrPrefix) {
				realKey := key[len(s.AttrPrefix):]
				sMsg.Attr[realKey] = val
			}
		}
	}
	return sMsg, nil
}
