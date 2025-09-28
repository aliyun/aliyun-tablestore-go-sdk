package exactlyOnce

import (
	"encoding/json"
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tunnel"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type userCheckpointer interface {
	// Specify the primary key and column names, return the channelId, sequenceInfo, and attribute column map before that row.
	getCheckpoint(id string, colNameToGet []string) (channelId string, sequenceInfo *tunnel.SequenceInfo,
		valueMap map[string]interface{}, err error)
	// Update the specified column's channelId, sequenceInfo. The condition is the expected condition for optimistic locking, and valueMap is the attribute column map.
	updateCheckpoint(id, channelId string, sequenceInfo *tunnel.SequenceInfo,
		condition *tablestore.RowCondition, valueMap map[string]interface{}) error
}

type Config struct {
	Endpoint  string
	Instance  string
	TableName string
	AkId      string
	AkSecret  string
}

var testConfig = Config{
	Endpoint:  "<Your instance endpoint>",
	Instance:  "<Your instance name>",
	TableName: "<Your table name>",
	AkId:      "<Your ak id>",
	AkSecret:  "<Your ak secret>",
}

func main() {
	tunnelClient := tunnel.NewTunnelClient(testConfig.Endpoint, testConfig.Instance,
		testConfig.AkId, testConfig.AkSecret)

	//open existing tunnel for scale or failover
	tunnelName := "exampleStreamTunnel"
	req := &tunnel.DescribeTunnelRequest{
		TableName:  testConfig.TableName,
		TunnelName: tunnelName,
	}
	resp, err := tunnelClient.DescribeTunnel(req)
	if err != nil {
		log.Fatal("create test tunnel failed", err)
	}
	log.Println("tunnel id is", resp.Tunnel.TunnelId)

	// UserCheckpointer interface based on OTS
	var checkpointer userCheckpointer //todo implementation

	//start consume tunnel
	workConfig := &tunnel.TunnelWorkerConfig{
		ProcessorFactory: &tunnel.SimpleProcessFactory{
			CustomValue: checkpointer,
			ProcessFunc: exactlyOnceIngestionCurrentState,
			ShutdownFunc: func(ctx *tunnel.ChannelContext) {
				fmt.Println("shutdown hook")
			},
		},
	}

	daemon := tunnel.NewTunnelDaemon(tunnelClient, resp.Tunnel.TunnelId, workConfig)
	go func() {
		err = daemon.Run()
		if err != nil {
			log.Fatal("tunnel worker fatal error: ", err)
		}
	}()

	{
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGINT)
		<-stop
		daemon.Close()
	}
}

// In the current version of exactly once, the epoch in record.SequenceInfo is not ready yet and is always 0. After partition splitting, comparisons need to be made based on changes in channelId.
// to distinguish the optimistic lock condition for this situation
func exactlyOnceIngestionCurrentState(ctx *tunnel.ChannelContext, records []*tunnel.Record) error {
	checkpointer := ctx.CustomValue.(userCheckpointer)
	inputChannelId := ctx.ChannelId

	for _, rec := range records {
		if rec.SequenceInfo == nil { // Only incremental data has SequenceInfo
			//unexpected base data record
			continue
		}
		id := rec.PrimaryKey.PrimaryKeys[0].Value.(string)
		instateCid, instateSeq, valueMap, err := checkpointer.getCheckpoint(id, nil)
		if err != nil {
			return err
		}
		duplicated, condition := checkRecordCurrentState(inputChannelId, instateCid, rec.SequenceInfo, instateSeq)
		if duplicated {
			continue //skip
		}
		//todo do something with valueMap
		fmt.Println("map size", len(valueMap))
		err = checkpointer.updateCheckpoint(id, inputChannelId, rec.SequenceInfo, condition, valueMap)
		if err != nil {
			return err
		}
	}
	fmt.Println("a round of records consumption finished")
	return nil
}

func checkRecordCurrentState(incomingCid, instateCid string, incomingSeq, instateSeq *tunnel.SequenceInfo) (duplicated bool, condition *tablestore.RowCondition) {
	condition = new(tablestore.RowCondition)
	if instateSeq == nil { // Data row does not exist
		condition.RowExistenceExpectation = tablestore.RowExistenceExpectation_EXPECT_NOT_EXIST
		return
	}
	if incomingCid == instateCid { // Currently, cid cannot be compared in size. If they are not equal, it is considered that the partition has split, and the data of the new partition is pushed (it could also be the old data from the old process under D...)
		if !tunnel.StreamRecordSequenceLess(instateSeq, incomingSeq) {
			duplicated = true
			return
		}
	}
	condition.RowExistenceExpectation = tablestore.RowExistenceExpectation_EXPECT_EXIST
	compositeCondition := tablestore.NewCompositeColumnCondition(tablestore.LO_AND)
	compositeCondition.AddFilter(tablestore.NewSingleColumnCondition("ChannelId", tablestore.CT_EQUAL, instateCid))
	seqBuf, _ := json.Marshal(instateSeq)
	compositeCondition.AddFilter(tablestore.NewSingleColumnCondition("SequenceInfo", tablestore.CT_EQUAL, seqBuf))
	condition.ColumnCondition = compositeCondition
	return
}

// In future versions, the epoch in record.SequenceInfo will increment as the partition splits, ensuring that the epoch of the sub-partition log is definitely greater than that of its parent partition log, so there's no need to worry about channelId anymore.
func exactlyOnceIngestionFinalState(ctx *tunnel.ChannelContext, records []*tunnel.Record) error {
	checkpointer := ctx.CustomValue.(userCheckpointer)

	for _, rec := range records {
		if rec.SequenceInfo == nil { // Only incremental data has SequenceInfo
			//unexpected base data record
			continue
		}
		id := rec.PrimaryKey.PrimaryKeys[0].Value.(string)
		_, instateSeq, valueMap, err := checkpointer.getCheckpoint(id, nil)
		if err != nil {
			return err
		}
		duplicated, condition := checkRecordFinalState(rec.SequenceInfo, instateSeq)
		if duplicated {
			continue //skip
		}
		//todo do something with valueMap
		fmt.Println("map size", len(valueMap))
		err = checkpointer.updateCheckpoint(id, "", rec.SequenceInfo, condition, valueMap)
		if err != nil {
			return err
		}
	}
	fmt.Println("a round of records consumption finished")
	return nil
}

func checkRecordFinalState(incomingSeq, instateSeq *tunnel.SequenceInfo) (duplicated bool, condition *tablestore.RowCondition) {
	condition = new(tablestore.RowCondition)
	if instateSeq == nil { // Data row does not exist
		condition.RowExistenceExpectation = tablestore.RowExistenceExpectation_EXPECT_NOT_EXIST
		return
	}
	if !tunnel.StreamRecordSequenceLess(instateSeq, incomingSeq) {
		duplicated = true
		return
	}
	condition.RowExistenceExpectation = tablestore.RowExistenceExpectation_EXPECT_EXIST
	seqBuf, _ := json.Marshal(instateSeq)
	condition.ColumnCondition = tablestore.NewSingleColumnCondition("SequenceInfo", tablestore.CT_EQUAL, seqBuf)
	return
}
