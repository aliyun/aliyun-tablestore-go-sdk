package tablestore

import (
	"errors"
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/testConfig"
	"github.com/golang/protobuf/proto"
	. "gopkg.in/check.v1"
	"math"
	"runtime"
	"strings"
	"time"
)

var (
	pk1Name   = "PkString"
	pk2Name   = "PkInt"
	attr1Name = "Attr1"
)

type GlobalTableSuite struct{}

var _ = Suite(&GlobalTableSuite{})

var defaultGlobalTableName string
var globalTableNamePrefix string

var gTableRegionAClient *TableStoreClient
var gTableRegionBClient *TableStoreClient

func (s *GlobalTableSuite) SetUpSuite(c *C) {
	globalTableNamePrefix = strings.Replace(runtime.Version(), ".", "", -1) + "_" + time.Now().Format("20060102150405")
	defaultGlobalTableName = globalTableNamePrefix + "_default_globaltable"

	// region A physical table needs exits first
	gTableRegionAClient = NewClient(testConfig.OtsEndpoint, testConfig.InstanceName, testConfig.OtsAccessId, testConfig.OtsAccessKey)
	if err := prepareSampleBaseTable(gTableRegionAClient, defaultGlobalTableName, c); err != nil {
		c.Errorf("create sample table of global table failed: %v", err)
	}

	// region B physical table needs not exists
	gTableRegionBClient = NewClient(testConfig.OtsGlobalTablePlacementEndpoint, testConfig.OtsGlobalTablePlacementInstanceName, testConfig.OtsAccessId, testConfig.OtsAccessKey)

}
func (s *GlobalTableSuite) TearDownSuite(c *C) {
	_ = retryWithTimeout(func() error {
		tableResp, descErr := gTableRegionAClient.DescribeTable(&DescribeTableRequest{TableName: defaultGlobalTableName})
		if descErr != nil {
			return descErr
		}
		baseTableMeta := tableResp.TableMeta
		if baseTableMeta == nil {
			return nil
		}
		if baseTableMeta.GlobalTableId == nil || *baseTableMeta.GlobalTableId == "" {
			return nil
		} else {
			return errors.New("GlobalTableId is not empty")
		}
	}, 3*time.Minute, 5*time.Second)

	_, delErr := gTableRegionAClient.DeleteTable(&DeleteTableRequest{
		TableName: defaultGlobalTableName,
	})
	if delErr != nil {
		c.Logf("[TearDownSuite] clear base table of global table failed: %+v", delErr)
	} else {
		c.Logf("[TearDownSuite] clear base table success")
	}
}

func (s *GlobalTableSuite) SetUpTest(c *C) {
}

// after each test method is executed, it is re-cleaned into a basic table
func (s *GlobalTableSuite) TearDownTest(c *C) {
	err := retryWithTimeout(func() error {
		return s.unbindAndDeletePlacementTable(defaultGlobalTableName)
	}, 6*time.Minute, 5*time.Second)
	if err != nil {
		c.Logf("[TearDownTest] clear placement table failed after retry: %+v", err)
	} else {
		c.Logf("[TearDownTest] clear placement table success")
	}
}

func (s *GlobalTableSuite) TestCreateGlobalTable(c *C) {
	createReq := &CreateGlobalTableRequest{
		BaseTable: &BaseTable{
			RegionId:     testConfig.Region,
			InstanceName: testConfig.InstanceName,
			TableName:    defaultGlobalTableName,
		},
		SyncMode:  SyncMode_Row,
		ServeMode: ServeMode_PrimarySecondary,
	}

	response, err := gTableRegionAClient.CreateGlobalTable(createReq)
	c.Assert(err, IsNil)
	c.Assert(response.RequestId, Not(Equals), "")

	waitingErr := waitingReconfCompleted(response.GlobalTableId, defaultGlobalTableName)
	c.Assert(waitingErr, IsNil)
}

func (s *GlobalTableSuite) TestBindGlobalTable(c *C) {
	globalTableId, err := prepareSimpleGlobalTable(gTableRegionAClient, testConfig.Region, testConfig.InstanceName, defaultGlobalTableName)
	c.Assert(err, IsNil)

	// binding
	bindReq := &BindGlobalTableRequest{
		GlobalTableId:   globalTableId,
		GlobalTableName: defaultGlobalTableName,
		Placements: []*Placement{
			{
				RegionId:     testConfig.OtsGlobalTablePlacementRegion,
				InstanceName: testConfig.OtsGlobalTablePlacementInstanceName,
				Writable:     false,
			},
		},
	}
	response, err := gTableRegionAClient.BindGlobalTable(bindReq)
	c.Assert(err, IsNil)
	c.Assert(response.RequestId, Not(Equals), "")

	waitingErr := waitingReconfCompleted(globalTableId, defaultGlobalTableName)
	c.Assert(waitingErr, IsNil)

	// write data and verify
	s.writeDataToPrimaryTable(c, "test_key", 123, "test_value")

	// verify that the data can be read in the secondary table
	s.verifyDataInPlacementTable(c, "test_key", 123, "test_value")
}

func (s *GlobalTableSuite) writeDataToPrimaryTable(c *C, pk1 string, pk2 int64, attrValue1 string) {
	putRowReq := &PutRowRequest{
		PutRowChange: &PutRowChange{
			TableName: defaultGlobalTableName,
			PrimaryKey: &PrimaryKey{
				PrimaryKeys: []*PrimaryKeyColumn{
					{
						ColumnName: pk1Name,
						Value:      pk1,
					},
					{
						ColumnName: pk2Name,
						Value:      pk2,
					},
				},
			},
			Columns: []AttributeColumn{
				{
					ColumnName: attr1Name,
					Value:      attrValue1,
				},
			},
			Condition: &RowCondition{
				RowExistenceExpectation: RowExistenceExpectation_IGNORE,
			},
		},
	}

	resp, err := gTableRegionAClient.PutRow(putRowReq)
	c.Assert(err, IsNil)
	c.Assert(resp.ResponseInfo.RequestId, Not(Equals), "")
}

func (s *GlobalTableSuite) verifyDataInPlacementTable(c *C, pk1 string, pk2 int64, attrValue1 string) {
	getRowReq := &GetRowRequest{
		SingleRowQueryCriteria: &SingleRowQueryCriteria{
			TableName: defaultGlobalTableName,
			PrimaryKey: &PrimaryKey{
				PrimaryKeys: []*PrimaryKeyColumn{
					{
						ColumnName: pk1Name,
						Value:      pk1,
					},
					{
						ColumnName: pk2Name,
						Value:      pk2,
					},
				},
			},
			MaxVersion: 1,
		},
	}

	verifyGetRowErr := retryWithTimeout(func() error {
		resp, getErr := gTableRegionBClient.GetRow(getRowReq)
		if getErr != nil {
			return getErr
		}

		// check whether the data was successfully read
		if len(resp.Columns) == 0 {
			return errors.New("no data found in placement table")
		}

		// verify data content
		found := false
		for _, col := range resp.Columns {
			if col.ColumnName == attr1Name && col.Value == attrValue1 {
				found = true
				break
			}
		}

		if !found {
			return errors.New("data content mismatch in placement table")
		}

		return nil
	}, 10*time.Minute, 3*time.Second)

	c.Assert(verifyGetRowErr, IsNil)
}

func (s *GlobalTableSuite) TestUnbindGlobalTable(c *C) {
	globalTableId, err := prepareGlobalTableWithPlacement(gTableRegionAClient, testConfig.Region, testConfig.InstanceName, defaultGlobalTableName,
		true, 30*time.Second, gTableRegionBClient)
	c.Assert(err, IsNil)

	// waiting for active
	waitingReconfErr := waitingReconfCompleted(globalTableId, defaultGlobalTableName)
	c.Assert(waitingReconfErr, IsNil)

	unbindReq := &UnbindGlobalTableRequest{
		GlobalTableId:   globalTableId,
		GlobalTableName: defaultGlobalTableName,
		Removals: []*Removal{
			{
				RegionId:     testConfig.OtsGlobalTablePlacementRegion,
				InstanceName: testConfig.OtsGlobalTablePlacementInstanceName,
			},
		},
	}
	unbResponse, err := gTableRegionAClient.UnbindGlobalTable(unbindReq)
	c.Assert(err, IsNil)
	c.Assert(unbResponse.RequestId, Not(Equals), "")

	waitingReconfErr = waitingReconfCompleted(globalTableId, defaultGlobalTableName)
	c.Assert(waitingReconfErr, IsNil)

	// write data to primary table after unbinding
	s.writeDataToPrimaryTable(c, "unbind_test_key", 456, "unbind_test_value")

	// verify that the data cannot be read from the placement table for 1 minute
	s.verifyDataNotInPlacementTable(c, "unbind_test_key", 456, "unbind_test_value")

	_, delAfterUnboundErr := gTableRegionBClient.DeleteTable(&DeleteTableRequest{TableName: defaultGlobalTableName})
	c.Assert(delAfterUnboundErr, IsNil)
}

func (s *GlobalTableSuite) verifyDataNotInPlacementTable(c *C, pk1 string, pk2 int64, attrValue1 string) {
	getRowReq := &GetRowRequest{
		SingleRowQueryCriteria: &SingleRowQueryCriteria{
			TableName: defaultGlobalTableName,
			PrimaryKey: &PrimaryKey{
				PrimaryKeys: []*PrimaryKeyColumn{
					{
						ColumnName: pk1Name,
						Value:      pk1,
					},
					{
						ColumnName: pk2Name,
						Value:      pk2,
					},
				},
			},
		},
	}

	// Verify that no data can be read within 1 minute.
	startTime := time.Now()
	timeout := 1 * time.Minute

	for {
		if time.Since(startTime) >= timeout {
			break
		}
		resp, getErr := gTableRegionBClient.GetRow(getRowReq)

		if getErr != nil {
			// continue to next attempt
		} else if len(resp.Columns) == 0 {
			// continue to next attempt
		} else {
			found := false
			for _, col := range resp.Columns {
				if col.ColumnName == attr1Name && col.Value == attrValue1 {
					found = true
					break
				}
			}

			if found {
				c.Errorf("Data should not be readable from placement table after unbinding, but found the data")
				return
			}
		}
		time.Sleep(3 * time.Second)
	}

	c.Logf("verified that data is not accessible from placement table for %v", timeout)
}

func (s *GlobalTableSuite) TestDescribeGlobalTable(c *C) {
	globalTableId, err := prepareSimpleGlobalTable(gTableRegionAClient, testConfig.Region, testConfig.InstanceName, defaultGlobalTableName)

	describeReq := &DescribeGlobalTableRequest{
		GlobalTableName: defaultGlobalTableName,
		GlobalTableId:   globalTableId,
	}
	response, err := gTableRegionAClient.DescribeGlobalTable(describeReq)
	c.Assert(err, IsNil)
	c.Assert(response.GlobalTableId, Not(Equals), "")
	c.Assert(len(response.PhyTables), Equals, 1)
}

func (s *GlobalTableSuite) TestUpdateGlobalTable(c *C) {
	globalTableId, prepareErr := prepareGlobalTableWithPlacement(gTableRegionAClient, testConfig.Region, testConfig.InstanceName, defaultGlobalTableName,
		true, 30*time.Second, gTableRegionBClient)
	c.Assert(prepareErr, IsNil)
	// waiting for active
	waitingReconfErr := waitingReconfCompleted(globalTableId, defaultGlobalTableName)
	c.Assert(waitingReconfErr, IsNil)

	updateReq := &UpdateGlobalTableRequest{
		GlobalTableId:   globalTableId,
		GlobalTableName: defaultGlobalTableName,
		PhyTable: UpdatePhyTable{
			RegionId:        testConfig.OtsGlobalTablePlacementRegion,
			InstanceName:    testConfig.OtsGlobalTablePlacementInstanceName,
			TableName:       defaultGlobalTableName,
			Writable:        proto.Bool(false),
			PrimaryEligible: proto.Bool(true),
		},
	}

	response, err := gTableRegionAClient.UpdateGlobalTable(updateReq)
	c.Assert(err, IsNil)
	c.Assert(response.RequestId, Not(Equals), "")
}

// placement table will be deleted, but the base table will not be deleted
func (s *GlobalTableSuite) unbindAndDeletePlacementTable(tableName string) error {
	describeReq := &DescribeGlobalTableRequest{
		GlobalTableName: tableName,
		PhyTable: &PhyTable{
			RegionId:     testConfig.Region,
			InstanceName: testConfig.InstanceName,
			TableName:    tableName,
		},
	}
	describeGlobalIdResp, err := gTableRegionAClient.DescribeGlobalTable(describeReq)
	if err != nil {
		return err
	}

	descResp, err := gTableRegionAClient.DescribeGlobalTable(
		&DescribeGlobalTableRequest{
			GlobalTableId:   describeGlobalIdResp.GlobalTableId,
			GlobalTableName: tableName},
	)
	if err != nil {
		return err
	}

	var removePlacements []*Removal
	var removePrimary *Removal
	for _, phyTable := range descResp.PhyTables {
		if phyTable.Role == "primary" {
			removePrimary = &Removal{
				RegionId:     phyTable.RegionId,
				InstanceName: phyTable.InstanceName,
			}
		} else {
			removePlacements = append(removePlacements, &Removal{
				RegionId:     phyTable.RegionId,
				InstanceName: phyTable.InstanceName,
			})
		}
	}

	if len(removePlacements) > 0 {
		unbindReq := &UnbindGlobalTableRequest{
			GlobalTableId:   descResp.GlobalTableId,
			GlobalTableName: tableName,
			Removals:        removePlacements,
		}
		_, unbErr := gTableRegionAClient.UnbindGlobalTable(unbindReq)
		if unbErr != nil {
			return unbErr
		}
		// waiting for placement table unbound
		waitingReconfErr := waitingReconfCompleted(descResp.GlobalTableId, defaultGlobalTableName)
		if waitingReconfErr != nil {
			return waitingReconfErr
		}
	}

	if removePrimary != nil {
		unbindReq := &UnbindGlobalTableRequest{
			GlobalTableId:   descResp.GlobalTableId,
			GlobalTableName: tableName,
			Removals:        []*Removal{removePrimary},
		}
		_, unbErr := gTableRegionAClient.UnbindGlobalTable(unbindReq)
		if unbErr != nil {
			return unbErr
		}

		// waiting for primary table unbound
		waitingReconfErr := waitingReconfCompleted(descResp.GlobalTableId, defaultGlobalTableName)
		if waitingReconfErr != nil {
			return waitingReconfErr
		}
	}

	// delete placement table, not delete base table
	for _, phyTable := range descResp.PhyTables {
		if phyTable.RegionId == testConfig.OtsGlobalTablePlacementRegion &&
			phyTable.InstanceName == testConfig.OtsGlobalTablePlacementInstanceName {
			_, delErr := gTableRegionBClient.DeleteTable(&DeleteTableRequest{
				TableName: phyTable.TableName,
			})
			if delErr != nil {
				return delErr
			} else {
				println("deleted placement table: " + phyTable.TableName + ", region: " + phyTable.RegionId)
			}
		}
	}
	return nil
}

func prepareSampleBaseTable(cli *TableStoreClient, tableName string, c *C) error {
	req := new(CreateTableRequest)
	tableMeta := new(TableMeta)
	tableMeta.TableName = tableName
	tableMeta.AddPrimaryKeyColumn(pk1Name, PrimaryKeyType_STRING)
	tableMeta.AddPrimaryKeyColumn(pk2Name, PrimaryKeyType_INTEGER)
	tableMeta.AddDefinedColumn(attr1Name, DefinedColumn_STRING)
	req.TableMeta = tableMeta
	req.TableOption = &TableOption{
		TimeToAlive:               -1,
		MaxVersion:                1,
		UpdateFullRow:             proto.Bool(true),
		DeviationCellVersionInSec: math.MaxInt32,
	}
	req.ReservedThroughput = &ReservedThroughput{
		Readcap:  0,
		Writecap: 0,
	}
	req.TableOption.UpdateFullRow = proto.Bool(true)
	_, err := cli.CreateTable(req)
	return err
}

func prepareSimpleGlobalTable(cli *TableStoreClient, region, instanceName, tableName string) (string, error) {
	createReq := &CreateGlobalTableRequest{
		BaseTable: &BaseTable{
			RegionId:     region,
			InstanceName: instanceName,
			TableName:    tableName,
		},
		SyncMode:  SyncMode_Row,
		ServeMode: ServeMode_PrimarySecondary,
	}

	response, err := cli.CreateGlobalTable(createReq)
	if err != nil {
		return "", err
	}

	return response.GlobalTableId, nil
}

func prepareGlobalTableWithPlacement(cli *TableStoreClient, region, instanceName, tableName string,
	deletePlaceTableIfExist bool, waitingAfterDelete time.Duration, placementCli *TableStoreClient) (string, error) {
	createReq := &CreateGlobalTableRequest{
		BaseTable: &BaseTable{
			RegionId:     region,
			InstanceName: instanceName,
			TableName:    tableName,
		},
		SyncMode:  SyncMode_Row,
		ServeMode: ServeMode_PrimarySecondary,
		Placements: []*Placement{
			{
				RegionId:     testConfig.OtsGlobalTablePlacementRegion,
				InstanceName: testConfig.OtsGlobalTablePlacementInstanceName,
				Writable:     false,
			},
		},
	}

	response, err := cli.CreateGlobalTable(createReq)
	if err != nil {
		if !strings.Contains(err.Error(), "already exist") {
			return "", err
		}

		if !deletePlaceTableIfExist {
			return "", err
		}
		// delete dirty data, and retry create global table
		deletePlacementTblReq := &DeleteTableRequest{
			TableName: tableName,
		}
		_, _ = placementCli.DeleteTable(deletePlacementTblReq)
		time.Sleep(waitingAfterDelete)

		retriedResponse, retriedErr := cli.CreateGlobalTable(createReq)
		if retriedErr != nil {
			return "", retriedErr
		}
		return retriedResponse.GlobalTableId, nil
	}

	return response.GlobalTableId, nil
}

func retryWithTimeout(operation func() error, timeout time.Duration, interval time.Duration) error {
	timeoutChan := time.After(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	err := operation()
	if err == nil {
		return nil
	}

	for {
		select {
		case <-timeoutChan:
			return fmt.Errorf("operation timed out after %v, last error: %+v", timeout, err)
		case <-ticker.C:
			err = operation()
			if err == nil {
				return nil
			}
		}
	}
}

func waitingReconfCompleted(globalTableId string, tableName string) error {
	return retryWithTimeout(func() error {
		waitingResp, descErr := gTableRegionAClient.DescribeGlobalTable(
			&DescribeGlobalTableRequest{
				GlobalTableId:   globalTableId,
				GlobalTableName: tableName,
			},
		)
		if descErr != nil {
			if strings.Contains(descErr.Error(), "not a global table") {
				return nil
			}
			return descErr
		}
		if waitingResp.Status == GlobalTableStatus_Reconf {
			return errors.New("still in reconf")
		}
		return nil
	}, 12*time.Minute, 3*time.Second)
}
