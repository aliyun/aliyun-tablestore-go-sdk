package tablestore

import (
	. "gopkg.in/check.v1"
)

type SseSpecificationSuite struct{}

var _ = Suite(&SseSpecificationSuite{})

func (s *SseSpecificationSuite) TestCheckValidArguments(c *C) {
	{
		sse := new(SSESpecification)
		sse.SetEnable(false)
		c.Assert(sse.CheckArguments(), IsNil)
	}
	{
		sse := new(SSESpecification)
		sse.SetEnable(true)
		sse.SetKeyType(SSE_KMS_SERVICE)
		c.Assert(sse.CheckArguments(), IsNil)
	}
	{
		sse := new(SSESpecification)
		sse.SetEnable(true)
		sse.SetKeyType(SSE_BYOK)
		sse.SetKeyId("test-key-id")
		sse.SetRoleArn("test-role-arn")
		c.Assert(sse.CheckArguments(), IsNil)
	}
}

func (s *SseSpecificationSuite) TestCheckInvalidArguments(c *C) {
	{
		var sse *SSESpecification
		err := sse.CheckArguments()
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "SSESpecification is nil")
	}
	{
		sse := new(SSESpecification)
		sse.SetEnable(false)
		sse.SetKeyType(SSE_KMS_SERVICE)
		err := sse.CheckArguments()
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "key type cannot be set when enable is false")
	}
	{
		sse := new(SSESpecification)
		sse.SetEnable(true)
		err := sse.CheckArguments()
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "key type is required when enable is true")
	}
	{
		sse := new(SSESpecification)
		sse.SetEnable(true)
		sse.SetKeyType(SSE_KMS_SERVICE)
		sse.SetKeyId("test-key-id")
		err := sse.CheckArguments()
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "key id and role arn cannot be set when key type is not SSE_BYOK")
	}
	{
		sse := new(SSESpecification)
		sse.SetEnable(true)
		sse.SetKeyType(SSE_KMS_SERVICE)
		sse.SetRoleArn("test-role-arn")
		err := sse.CheckArguments()
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "key id and role arn cannot be set when key type is not SSE_BYOK")
	}
	{
		sse := new(SSESpecification)
		sse.SetEnable(true)
		sse.SetKeyType(SSE_BYOK)
		err := sse.CheckArguments()
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "key id and role arn are required when key type is not SSE_KMS_SERVICE")
	}
	{
		sse := new(SSESpecification)
		sse.SetEnable(true)
		sse.SetKeyType(SSE_BYOK)
		sse.SetKeyId("test-key-id")
		err := sse.CheckArguments()
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "key id and role arn are required when key type is not SSE_KMS_SERVICE")
	}
	{
		sse := new(SSESpecification)
		sse.SetEnable(true)
		sse.SetKeyType(SSE_BYOK)
		sse.SetRoleArn("test-role-arn")
		err := sse.CheckArguments()
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, "key id and role arn are required when key type is not SSE_KMS_SERVICE")
	}
}
