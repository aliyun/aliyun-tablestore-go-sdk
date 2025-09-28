package search

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

// Generate a random float32 array with the specified number of elements, each between [lowerBound, upperBound).
func generateRandomFloats(numFloats int, lowerBound, upperBound float32) []float32 {
	rand.New(rand.NewSource(time.Now().Unix()))
	randomFloats := make([]float32, numFloats)

	for i := 0; i < numFloats; i++ {
		randomFloats[i] = lowerBound + rand.Float32()*(upperBound-lowerBound)
	}

	return randomFloats
}

// Generate a random integer within the range [min, max].
func generateRandomInt(min, max int) int {
	rand.New(rand.NewSource(time.Now().Unix()))
	return rand.Intn(max-min+1) + min
}

func TestFloat32ToBytes(t *testing.T) {
	test := []struct {
		name        string
		vector      []float32
		expectValue []byte
		expectError error
	}{
		{
			name:        "success float32",
			vector:      []float32{1.0, 2.0, 3.0},
			expectValue: []byte{0, 0, 128, 63, 0, 0, 0, 64, 0, 0, 64, 64},
			expectError: nil,
		},
		{
			name:        "empty float32",
			vector:      []float32{},
			expectValue: nil,
			expectError: errors.New("vector is null or empty"),
		},
		{
			name:        "complex float32",
			vector:      []float32{1.1, 2.22, 3.333, 4.4444},
			expectValue: []byte{0xcd, 0xcc, 0x8c, 0x3f, 0x7b, 0x14, 0xe, 0x40, 0xdf, 0x4f, 0x55, 0x40, 0x86, 0x38, 0x8e, 0x40},
			expectError: nil,
		},
		{
			name:        "nil input",
			vector:      nil,
			expectValue: nil,
			expectError: errors.New("vector is null or empty"),
		},
	}
	for _, v := range test {
		actualValue, err := Float32ToBytes(v.vector)
		if err != nil {
			assert.Equal(t, v.expectError, err)
		}
		if actualValue != nil {
			assert.Equal(t, v.expectValue, actualValue)
		}
	}
}

func TestToFloat32(t *testing.T) {
	test := []struct {
		name        string
		data        []byte
		expectValue []float32
		expectError error
	}{
		{
			name:        "success byte",
			data:        []byte{0, 0, 128, 63, 0, 0, 0, 64, 0, 0, 64, 64},
			expectValue: []float32{1.0, 2.0, 3.0},
			expectError: nil,
		},
		{
			name:        "empty byte",
			data:        []byte{},
			expectValue: nil,
			expectError: errors.New("bytes length is not multiple of 4(SIZE_OF_FLOAT32) or length is 0"),
		},
		{
			name:        "lose byte",
			data:        []byte{0, 0, 128, 63, 0, 0, 0, 64, 0, 0, 64, 64, 0},
			expectValue: nil,
			expectError: errors.New("bytes length is not multiple of 4(SIZE_OF_FLOAT32) or length is 0"),
		},
		{
			name:        "nil input",
			data:        nil,
			expectValue: nil,
			expectError: errors.New("bytes is null"),
		},
	}
	for _, v := range test {
		actualValue, err := ToFloat32(v.data)
		if err != nil {
			assert.Equal(t, v.expectError, err)
		}
		if actualValue != nil {
			assert.Equal(t, v.expectValue, actualValue)
		}
	}
}

func TestFloat32AndBytesConvert(t *testing.T) {
	floats := generateRandomFloats(generateRandomInt(100, 1024), 0, 1)
	bytes, err := Float32ToBytes(floats)
	assert.Nil(t, err)
	toFloat32, err := ToFloat32(bytes)
	assert.Nil(t, err)
	assert.Equal(t, floats, toFloat32)
}
