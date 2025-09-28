package sample

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/common"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
)

func GetClientWithSignatureV2(endpoint, instanceName, accessKeyID, accessKeySecret string) *tablestore.TableStoreClient {
	return tablestore.NewClient(endpoint, instanceName, accessKeyID, accessKeySecret)
}

func GetClientWithSignatureV2ByDefaultCredentialsProvider(endpoint, instanceName, accessKeyID, accessKeySecret, securityToken string) *tablestore.TableStoreClient {
	provider := &common.DefaultCredentialsProvider{AccessKeyID: accessKeyID, AccessKeySecret: accessKeySecret, SecurityToken: securityToken}

	return tablestore.NewClientWithCredentialsProvider(endpoint, instanceName, provider, nil)
}

func GetClientWithSignatureV4ByCreateByCredentials(endpoint, instanceName, accessKeyID, accessKeySecret, securityToken, region string) *tablestore.TableStoreClient {
	defaultCredentialsProvider := &common.DefaultCredentialsProvider{AccessKeyID: accessKeyID, AccessKeySecret: accessKeySecret, SecurityToken: securityToken}
	provider := common.CreateByCredentials(defaultCredentialsProvider.GetCredentials(), region)

	return tablestore.NewClientWithCredentialsProvider(endpoint, instanceName, provider, nil)
}

func GetClientWithSignatureV4BySigningAccessKey(endpoint, instanceName, accessKeyID, securityToken, region, v4SigningAccessKey string) *tablestore.TableStoreClient {
	signDate := common.GetFormattedDate()
	provider := &common.V4Credentials{AccessKeyID: accessKeyID, V4SigningStsToken: securityToken, Region: region, SigningDate: signDate, V4SigningAccessKey: v4SigningAccessKey}

	return tablestore.NewClientWithCredentialsProvider(endpoint, instanceName, provider, nil)
}
