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
	// Create a table with server-side encryption disabled
	deleteTableIfExist(client, TABLE_NAME_DISABLE)
	createTableDisableSse(client, TABLE_NAME_DISABLE)

	// Create a table with server-side encryption (service master key) enabled
	deleteTableIfExist(client, TABLE_NAME_KMS_SERVICE)
	createTableKmsService(client, TABLE_NAME_KMS_SERVICE)

	// Create a table with server-side encryption (user master key) enabled
	deleteTableIfExist(client, TABLE_NAME_BYOK)
	createTableByok(client, TABLE_NAME_BYOK, BYOK_KEY_ID, BYOK_ROLE_ARN)

	// View table properties
	describeTable(client, TABLE_NAME_DISABLE)
	describeTable(client, TABLE_NAME_KMS_SERVICE)
	describeTable(client, TABLE_NAME_BYOK)

	// Wait for the table to load.
	time.Sleep(10 * time.Second)

	// Write one row of data each
	putRow(client, TABLE_NAME_DISABLE, "pkValue")
	putRow(client, TABLE_NAME_KMS_SERVICE, "pkValue")
	putRow(client, TABLE_NAME_BYOK, "pkValue")

	// Read the data of each row separately.
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
	// Disable server-side encryption
	sseSpec := new(tablestore.SSESpecification)
	sseSpec.SetEnable(false)

	createTable(client, tableName, sseSpec)
}

func createTableKmsService(client *tablestore.TableStoreClient, tableName string) {
	// Enable the server-side encryption feature, using the service master key of KMS.
	// Make sure that the KMS service has been activated in the corresponding region.
	sseSpec := new(tablestore.SSESpecification)
	sseSpec.SetEnable(true)
	sseSpec.SetKeyType(tablestore.SSE_KMS_SERVICE)

	createTable(client, tableName, sseSpec)
}

func createTableByok(client *tablestore.TableStoreClient, tableName string, keyId string, roleArn string) {
	// Enable the server-side encryption feature, using the user's main key in KMS
	// Ensure that the keyId is valid and not disabled, and that the roleArn has been granted temporary access permissions for this keyId.
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
