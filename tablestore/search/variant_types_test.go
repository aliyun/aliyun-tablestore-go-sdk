package search

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore/search/model"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

func TestAsLong(t *testing.T) {
	for i := 0; i < 1000; i++ {
		a := rand.Int63()
		b := -rand.Int63()
		var valueTest int64
		var err error
		valueTest, err = AsInteger(VTInteger(a))
		if err != nil{
			return
		}
		assert.Equal(t, valueTest, a)
		valueTest, err = AsInteger(VTInteger(b))
		if err != nil{
			return
		}
		assert.Equal(t, valueTest, b)
		valueTest, err = AsInteger(VTInteger(0))
		if err != nil{
			return
		}
		assert.Equal(t, valueTest, int64(0))
	}
}

func TestAsDouble(t *testing.T) {
	for i := 0; i < 1000; i++ {
		a := rand.Float64()
		b := -rand.Float64()
		var valueTest float64
		var err error
		valueTest, err = AsDouble(VTDouble(a))
		if err != nil{
			return
		}
		assert.Equal(t, valueTest, a)
		valueTest, err = AsDouble(VTDouble(b))
		if err != nil{
			return
		}
		assert.Equal(t, valueTest, b)
		valueTest, err = AsDouble(VTDouble(0))
		if err != nil{
			return
		}
		assert.Equal(t, valueTest, float64(0))
	}
}

func TestAsString(t *testing.T) {
	for i := 0; i < 10000; i++ {
		n := rand.Intn(1000) + 5
		str := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
		bytes := []byte(str)
		var result []byte
		for j := 0; j < n; j++ {
			result = append(result, bytes[rand.Intn(len(bytes))])
		}
		valueTest, err := AsString(VTString(string(result)))
		if err != nil {
			return
		}
		assert.Equal(t, valueTest, string(result))
	}
}

func TestAsBoolean(t *testing.T) {
	bytes := make([]byte, 2)
	bytes[1] = 1
	var valueTest bool
	var err error
	valueTest, err = AsBoolean(bytes)
	if err != nil {
		return
	}
	assert.Equal(t, valueTest, true)
	bytes[1] = 0
	valueTest, err = AsBoolean(bytes)
	if err != nil {
		return
	}
	assert.Equal(t, valueTest, false)
}

func TestForceConvertToDestColumnValue(t *testing.T) {
	for i := 0; i < 10000; i++ {
		if i % 4 == 0 {
			a := rand.Int63()
			b := -rand.Int63()
			columnValue, err := ForceConvertToDestColumnValue(VTInteger(a))
			if err != nil {
				return
			}
			assert.Equal(t, model.ColumnType_INTEGER, columnValue.Type)
			assert.Equal(t, a, columnValue.Value)
			columnValue, err = ForceConvertToDestColumnValue(VTInteger(b))
			if err != nil {
				return
			}
			assert.Equal(t, model.ColumnType_INTEGER, columnValue.Type)
			assert.Equal(t, b, columnValue.Value)
			var bytes = make([]byte, 1)
			bytes[0] = 55
			columnValue, err = ForceConvertToDestColumnValue(bytes)
			assert.Equal(t, "type must be string/int64/float64/boolean", err.Error())
		} else if i % 4 == 1 {
			a := rand.Float64()
			b := -rand.Float64()
			columnValue, err := ForceConvertToDestColumnValue(VTDouble(a))
			if err != nil {
				return
			}
			assert.Equal(t, model.ColumnType_DOUBLE, columnValue.Type)
			assert.Equal(t, a, columnValue.Value)
			columnValue, err = ForceConvertToDestColumnValue(VTDouble(b))
			if err != nil {
				return
			}
			assert.Equal(t, model.ColumnType_DOUBLE, columnValue.Type)
			assert.Equal(t, b, columnValue.Value)
		} else if i % 4 == 2 {
			n := rand.Intn(1000) + 5
			str := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
			bytes := []byte(str)
			var result []byte
			for j := 0; j < n; j++ {
				result = append(result, bytes[rand.Intn(len(bytes))])
			}
			columnValue, err := ForceConvertToDestColumnValue(VTString(string(result)))
			if err != nil {
				return
			}
			assert.Equal(t, model.ColumnType_STRING, columnValue.Type)
			assert.Equal(t, string(result) ,columnValue.Value)
		} else if i % 4 == 3 {
			columnValue, err := ForceConvertToDestColumnValue(VTBoolean(false))
			if err != nil {
				return
			}
			assert.Equal(t, model.ColumnType_BOOLEAN, columnValue.Type)
			assert.Equal(t, false, columnValue.Value)
			columnValue, err = ForceConvertToDestColumnValue(VTBoolean(true))
			if err != nil {
				return
			}
			assert.Equal(t, model.ColumnType_BOOLEAN, columnValue.Type)
			assert.Equal(t, true, columnValue.Value)
		}
	}
}