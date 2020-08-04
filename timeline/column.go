package timeline

import (
	"github.com/lanjingren/aliyun-tablestore-go-sdk/tablestore"
)

type ColumnMap struct {
	int64Fields   map[string]int64
	bytesFields   map[string][]byte
	stringFields  map[string]string
	float64Fields map[string]float64
	boolFields    map[string]bool

	fields map[string]struct{}
}

func NewColumnMap() *ColumnMap {
	return &ColumnMap{
		int64Fields:   make(map[string]int64),
		bytesFields:   make(map[string][]byte),
		stringFields:  make(map[string]string),
		float64Fields: make(map[string]float64),
		boolFields:    make(map[string]bool),
		fields:        make(map[string]struct{}),
	}
}

func (cm *ColumnMap) ToMap() map[string]interface{} {
	var mm = make(map[string]interface{})
	for k, v := range cm.int64Fields {
		mm[k] = v
	}
	for k, v := range cm.stringFields {
		mm[k] = v
	}
	for k, v := range cm.float64Fields {
		mm[k] = v
	}
	for k, v := range cm.bytesFields {
		mm[k] = v
	}
	for k, v := range cm.boolFields {
		mm[k] = v
	}
	return mm
}

func loadColumnMap(pk *tablestore.PrimaryKey, attrs []*tablestore.AttributeColumn) *ColumnMap {
	cols := NewColumnMap()
	for _, attr := range attrs {
		if val, ok := attr.Value.(string); ok {
			cols.AddStringColumn(attr.ColumnName, val)
		}
		if val, ok := attr.Value.([]byte); ok {
			cols.AddBytesColumn(attr.ColumnName, val)
		}
		if val, ok := attr.Value.(int64); ok {
			cols.AddInt64Column(attr.ColumnName, val)
		}
		if val, ok := attr.Value.(float64); ok {
			cols.AddFloat64Column(attr.ColumnName, val)
		}
		if val, ok := attr.Value.(bool); ok {
			cols.AddBoolColumn(attr.ColumnName, val)
		}
	}
	for _, attr := range pk.PrimaryKeys {
		if val, ok := attr.Value.(string); ok {
			cols.AddStringColumn(attr.ColumnName, val)
		}
		if val, ok := attr.Value.([]byte); ok {
			cols.AddBytesColumn(attr.ColumnName, val)
		}
		if val, ok := attr.Value.(int64); ok {
			cols.AddInt64Column(attr.ColumnName, val)
		}
	}
	return cols
}

func (cm *ColumnMap) GetInt64Column(key string) int64 {
	return cm.int64Fields[key]
}

func (cm *ColumnMap) GetStringColumn(key string) string {
	return cm.stringFields[key]
}

func (cm *ColumnMap) AddInt64Column(key string, val int64) {
	if _, ok := cm.fields[key]; ok {
		panic("duplicate add column: " + key)
	}
	cm.fields[key] = struct{}{}
	cm.int64Fields[key] = val
}

func (cm *ColumnMap) AddStringColumn(key string, val string) {
	if _, ok := cm.fields[key]; ok {
		panic("duplicate add column: " + key)
	}
	cm.fields[key] = struct{}{}
	cm.stringFields[key] = val
}

func (cm *ColumnMap) AddBytesColumn(key string, val []byte) {
	if _, ok := cm.fields[key]; ok {
		panic("duplicate add column: " + key)
	}
	cm.fields[key] = struct{}{}
	cm.bytesFields[key] = val
}

func (cm *ColumnMap) AddFloat64Column(key string, val float64) {
	if _, ok := cm.fields[key]; ok {
		panic("duplicate add column: " + key)
	}
	cm.fields[key] = struct{}{}
	cm.float64Fields[key] = val
}

func (cm *ColumnMap) AddBoolColumn(key string, val bool) {
	if _, ok := cm.fields[key]; ok {
		panic("duplicate add column: " + key)
	}
	cm.fields[key] = struct{}{}
	cm.boolFields[key] = val
}

func (cm *ColumnMap) AddAnyColumn(key string, val interface{}) {
	switch v := val.(type) {
	case int:
		cm.AddInt64Column(key, int64(v))
	case int8:
		cm.AddInt64Column(key, int64(v))
	case int16:
		cm.AddInt64Column(key, int64(v))
	case int32:
		cm.AddInt64Column(key, int64(v))
	case int64:
		cm.AddInt64Column(key, v)
	case uint:
		cm.AddInt64Column(key, int64(v))
	case uint8:
		cm.AddInt64Column(key, int64(v))
	case uint16:
		cm.AddInt64Column(key, int64(v))
	case uint32:
		cm.AddInt64Column(key, int64(v))
	case uint64:
		cm.AddInt64Column(key, int64(v))
	case string:
		cm.AddStringColumn(key, v)
	case float64:
		cm.AddFloat64Column(key, v)
	case float32:
		cm.AddFloat64Column(key, float64(v))
	case bool:
		cm.AddBoolColumn(key, v)
	case []byte:
		cm.AddBytesColumn(key, v)
	default:
		panic("invalid value type")
	}
}

func FromMap(mm map[string]interface{}) *ColumnMap {
	cm := NewColumnMap()
	for key, val := range mm {
		cm.AddAnyColumn(key, val)
	}
	return cm
}
