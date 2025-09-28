package search

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
)

// Float32ToBytes converts a vector of float32 type to a byte array.
func Float32ToBytes(vector []float32) ([]byte, error) {
	if len(vector) == 0 {
		return nil, errors.New("vector is null or empty")
	}
	data := make([]byte, 4*len(vector))

	for i, v := range vector {
		binary.LittleEndian.PutUint32(data[i*4:(i+1)*4], math.Float32bits(v))
	}
	return data, nil
}

// ToFloat32 converts a byte array to a vector of float32 type
func ToFloat32(data []byte) ([]float32, error) {
	if data == nil {
		return nil, errors.New("bytes is null")
	}
	if len(data)%4 != 0 || len(data) == 0 {
		return nil, errors.New("bytes length is not multiple of 4(SIZE_OF_FLOAT32) or length is 0")
	}
	floats := make([]float32, len(data)/4)
	buf := bytes.NewReader(data)

	for i := range floats {
		if err := binary.Read(buf, binary.LittleEndian, &floats[i]); err != nil {
			return nil, err
		}
	}

	return floats, nil
}
