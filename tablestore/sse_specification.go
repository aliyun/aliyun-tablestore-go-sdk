package tablestore

import "errors"

// Indicates the type of server-side encryption key
type SSEKeyType int

func (t *SSEKeyType) String() string {
	switch *t {
	case SSE_KMS_SERVICE:
		return "SSE_KMS_SERVICE"
	case SSE_BYOK:
		return "SSE_BYOK"
	default:
		return ""
	}
}

const (
	// Use the service master key of KMS
	SSE_KMS_SERVICE SSEKeyType = iota

	// Use the KMS customer master key, supporting user-defined key upload.
	SSE_BYOK
)

type SSESpecification struct {
	// Whether to enable server-side encryption
	Enable bool

	// When server-side encryption is enabled, this parameter is used to set the key type.
	KeyType *SSEKeyType

	// When server-side encryption is enabled and the key type is BYOK, this parameter is used to specify the id of the KMS user master key.
	KeyId *string

	// When server-side encryption is enabled and the key type is BYOK, you need to authorize Table Store to obtain a temporary access token through the STS service to access the KMS user master key.
	// This parameter is used to specify the global resource descriptor for the RAM role created for this purpose.
	RoleArn *string
}

func (sse *SSESpecification) CheckArguments() error {
	if sse == nil {
		return errors.New("SSESpecification is nil")
	}
	if sse.Enable {
		if sse.KeyType == nil {
			return errors.New("key type is required when enable is true")
		} else {
			if *sse.KeyType != SSE_BYOK {
				if sse.KeyId != nil || sse.RoleArn != nil {
					return errors.New("key id and role arn cannot be set when key type is not SSE_BYOK")
				}
			}

			if *sse.KeyType != SSE_KMS_SERVICE {
				if sse.KeyId == nil || sse.RoleArn == nil {
					return errors.New("key id and role arn are required when key type is not SSE_KMS_SERVICE")
				}
			}
		}
	} else {
		if sse.KeyType != nil {
			return errors.New("key type cannot be set when enable is false")
		}
	}

	return nil
}

func (sse *SSESpecification) SetEnable(enable bool) {
	sse.Enable = enable
}

func (sse *SSESpecification) SetKeyType(keyType SSEKeyType) {
	sse.KeyType = &keyType
}

func (sse *SSESpecification) SetKeyId(keyId string) {
	sse.KeyId = &keyId
}

func (sse *SSESpecification) SetRoleArn(roleArn string) {
	sse.RoleArn = &roleArn
}

type SSEDetails struct {
	// Whether to enable server-side encryption
	Enable bool

	// Key type, valid when server-side encryption is enabled
	KeyType SSEKeyType

	// The ID of the master key in KMS, you can audit the usage of the key in the KMS system based on the keyId.
	// Valid when server-side encryption is enabled
	KeyId string

	// Authorizes the global resource descriptor for temporarily accessing the KMS user's main key in Table Store.
	// Valid when server-side encryption is enabled and the key type is SSE_BYOK.
	RoleArn string
}
