package tablestore

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCheckValidArguments(t *testing.T) {
	{
		sse := new(SSESpecification)
		sse.SetEnable(false)
		err := sse.CheckArguments()
		assert.Equal(t, nil, err)
	}
	{
		sse := new(SSESpecification)
		sse.SetEnable(true)
		sse.SetKeyType(SSE_KMS_SERVICE)
		err := sse.CheckArguments()
		assert.Equal(t, nil, err)
	}
	{
		sse := new(SSESpecification)
		sse.SetEnable(true)
		sse.SetKeyType(SSE_BYOK)
		sse.SetKeyId("test-key-id")
		sse.SetRoleArn("test-role-arn")
		err := sse.CheckArguments()
		assert.Equal(t, nil, err)
	}
}

func TestCheckInvalidArguments(t *testing.T) {
	{
		var sse *SSESpecification
		err := sse.CheckArguments()
		assert.NotNil(t, err)
		assert.Equal(t, "SSESpecification is nil", err.Error())
	}
	{
		sse := new(SSESpecification)
		sse.SetEnable(false)
		sse.SetKeyType(SSE_KMS_SERVICE)
		err := sse.CheckArguments()
		assert.NotNil(t, err)
		assert.Equal(t, "key type cannot be set when enable is false", err.Error())
	}
	{
		sse := new(SSESpecification)
		sse.SetEnable(true)
		err := sse.CheckArguments()
		assert.NotNil(t, err)
		assert.Equal(t, "key type is required when enable is true", err.Error())
	}
	{
		sse := new(SSESpecification)
		sse.SetEnable(true)
		sse.SetKeyType(SSE_KMS_SERVICE)
		sse.SetKeyId("test-key-id")
		err := sse.CheckArguments()
		assert.NotNil(t, err)
		assert.Equal(t, "key id and role arn cannot be set when key type is not SSE_BYOK", err.Error())
	}
	{
		sse := new(SSESpecification)
		sse.SetEnable(true)
		sse.SetKeyType(SSE_KMS_SERVICE)
		sse.SetRoleArn("test-role-arn")
		err := sse.CheckArguments()
		assert.NotNil(t, err)
		assert.Equal(t, "key id and role arn cannot be set when key type is not SSE_BYOK", err.Error())
	}
	{
		sse := new(SSESpecification)
		sse.SetEnable(true)
		sse.SetKeyType(SSE_BYOK)
		err := sse.CheckArguments()
		assert.NotNil(t, err)
		assert.Equal(t, "key id and role arn are required when key type is not SSE_KMS_SERVICE", err.Error())
	}
	{
		sse := new(SSESpecification)
		sse.SetEnable(true)
		sse.SetKeyType(SSE_BYOK)
		sse.SetKeyId("test-key-id")
		err := sse.CheckArguments()
		assert.NotNil(t, err)
		assert.Equal(t, "key id and role arn are required when key type is not SSE_KMS_SERVICE", err.Error())
	}
	{
		sse := new(SSESpecification)
		sse.SetEnable(true)
		sse.SetKeyType(SSE_BYOK)
		sse.SetRoleArn("test-role-arn")
		err := sse.CheckArguments()
		assert.NotNil(t, err)
		assert.Equal(t, "key id and role arn are required when key type is not SSE_KMS_SERVICE", err.Error())
	}
}
