package sample

import (
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"time"
)

const (
	TABLE_NAME_DISABLE     = "disableSseSampleTable"
	TABLE_NAME_KMS_SERVICE = "kmsServiceSampleTable"
	TABLE_NAME_BYOK        = "byokSampleTable"
	PRIMARY_KEY_NAME       = "pk"

	BYOK_KEY_ID   = ""
	BYOK_ROLE_ARN = "acs:ram::<aliuid>:role/kms-ots-test"
)

func ServerSideEncryptionSample(client *tablestore.TableStoreClient) {
	// 创建关闭服务器端加密功能的表
	deleteTableIfExist(client, TABLE_NAME_DISABLE)
	createTableDisableSse(client, TABLE_NAME_DISABLE)

	// 创建开启服务器端加密功能(服务主秘钥)的表
	deleteTableIfExist(client, TABLE_NAME_KMS_SERVICE)
	createTableKmsService(client, TABLE_NAME_KMS_SERVICE)

	// 创建开启服务器端加密功能(用户主秘钥)的表
	deleteTableIfExist(client, TABLE_NAME_BYOK)
	createTableByok(client, TABLE_NAME_BYOK, BYOK_KEY_ID, BYOK_ROLE_ARN)

	// 查看表的属性
	describeTable(client, TABLE_NAME_DISABLE)
	describeTable(client, TABLE_NAME_KMS_SERVICE)
	describeTable(client, TABLE_NAME_BYOK)

	// 等待表load完毕.
	time.Sleep(10 * time.Second)

	// 各写入一行数据
	putRow(client, TABLE_NAME_DISABLE, "pkValue")
	putRow(client, TABLE_NAME_KMS_SERVICE, "pkValue")
	putRow(client, TABLE_NAME_BYOK, "pkValue")

	// 各读取该行数据
	getRow(client, TABLE_NAME_DISABLE, "pkValue")
	getRow(client, TABLE_NAME_KMS_SERVICE, "pkValue")
	getRow(client, TABLE_NAME_BYOK, "pkValue")
}

func deleteTableIfExist(client *tablestore.TableStoreClient, tableName string) {
	_, err := client.DeleteTable(&tablestore.DeleteTableRequest{
		TableName: tableName,
	})
	if err != nil {
		fmt.Println("DeleteTable failed", tableName, err.Error())
	}
}

func createTable(client *tablestore.TableStoreClient, tableName string, sseSpec *tablestore.SSESpecification) {
	createtableRequest := new(tablestore.CreateTableRequest)
	tableMeta := new(tablestore.TableMeta)
	tableMeta.TableName = tableName
	tableMeta.AddPrimaryKeyColumn(PRIMARY_KEY_NAME, tablestore.PrimaryKeyType_STRING)
	tableOption := new(tablestore.TableOption)
	tableOption.TimeToAlive = -1
	tableOption.MaxVersion = 3
	reservedThroughput := new(tablestore.ReservedThroughput)
	reservedThroughput.Readcap = 0
	reservedThroughput.Writecap = 0
	createtableRequest.TableMeta = tableMeta
	createtableRequest.TableOption = tableOption
	createtableRequest.ReservedThroughput = reservedThroughput
	createtableRequest.SSESpecification = sseSpec

	_, err := client.CreateTable(createtableRequest)
	if err != nil {
		fmt.Println("CreateTable failed", tableName, err.Error())
	}
}

func createTableDisableSse(client *tablestore.TableStoreClient, tableName string) {
	// 关闭服务器端加密功能
	sseSpec := new(tablestore.SSESpecification)
	sseSpec.SetEnable(false)

	createTable(client, tableName, sseSpec)
}

func createTableKmsService(client *tablestore.TableStoreClient, tableName string) {
	// 打开服务器端加密功能，使用KMS的服务主密钥
	// 需要确保已经在所在区域开通了KMS服务
	sseSpec := new(tablestore.SSESpecification)
	sseSpec.SetEnable(true)
	sseSpec.SetKeyType(tablestore.SSE_KMS_SERVICE)

	createTable(client, tableName, sseSpec)
}

func createTableByok(client *tablestore.TableStoreClient, tableName string, keyId string, roleArn string) {
	// 打开服务器端加密功能，使用KMS的用户主密钥
	// 需要确保keyId合法有效且未被禁用，同时roleArn被授予了临时访问该keyId的权限
	sseSpec := new(tablestore.SSESpecification)
	sseSpec.SetEnable(true)
	sseSpec.SetKeyType(tablestore.SSE_BYOK)
	sseSpec.SetKeyId(keyId)
	sseSpec.SetRoleArn(roleArn)

	createTable(client, tableName, sseSpec)
}

func describeTable(client *tablestore.TableStoreClient, tableName string) {
	resp, err := client.DescribeTable(&tablestore.DescribeTableRequest{
		TableName: tableName,
	})
	if err != nil {
		fmt.Println("describe table failed", tableName, err.Error())
		return
	}
	fmt.Println("表的名称：" + resp.TableMeta.TableName)
	sseDetails := resp.SSEDetails
	if sseDetails.Enable {
		fmt.Println("表是否开启服务器端加密功能：是")
		fmt.Println("表的加密秘钥类型：", sseDetails.KeyType.String())
		fmt.Println("表的加密主密钥id：", sseDetails.KeyId)
		if sseDetails.KeyType == tablestore.SSE_BYOK {
			fmt.Println("表的全局资源描述符：" + sseDetails.RoleArn)
		}
	} else {
		fmt.Println("表是否开启服务器端加密功能：否")
	}

}

func putRow(client *tablestore.TableStoreClient, tableName string, pkValue string) {
	putRowRequest := new(tablestore.PutRowRequest)
	putRowChange := new(tablestore.PutRowChange)
	putRowChange.TableName = tableName
	putPk := new(tablestore.PrimaryKey)
	putPk.AddPrimaryKeyColumn(PRIMARY_KEY_NAME, pkValue)

	putRowChange.PrimaryKey = putPk
	putRowChange.AddColumn("price", int64(5120))
	putRowChange.SetCondition(tablestore.RowExistenceExpectation_IGNORE)
	putRowRequest.PutRowChange = putRowChange
	_, err := client.PutRow(putRowRequest)
	if err != nil {
		fmt.Println("PutRow failed", tableName, err.Error())
	}
}

func getRow(client *tablestore.TableStoreClient, tableName string, pkValue string) {
	getRowRequest := new(tablestore.GetRowRequest)
	criteria := new(tablestore.SingleRowQueryCriteria)
	putPk := new(tablestore.PrimaryKey)
	putPk.AddPrimaryKeyColumn(PRIMARY_KEY_NAME, pkValue)

	criteria.PrimaryKey = putPk
	getRowRequest.SingleRowQueryCriteria = criteria
	getRowRequest.SingleRowQueryCriteria.TableName = tableName
	getRowRequest.SingleRowQueryCriteria.MaxVersion = 1
	getResp, err := client.GetRow(getRowRequest)

	if err != nil {
		fmt.Println("GetRow failed", tableName, err)
	} else {
		colmap := getResp.GetColumnMap()
		fmt.Println(tableName, "length is ", len(colmap.Columns))
		fmt.Println("get row col0 result is ", getResp.Columns[0].ColumnName, getResp.Columns[0].Value)
	}
}
