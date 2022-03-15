package main

import (
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tunnel"
	"log"
	"os"
	"os/signal"
	"syscall"
)

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
	tunnelName := "exampleTunnel"
	req := &tunnel.DescribeTunnelRequest{
		TableName:  testConfig.TableName,
		TunnelName: tunnelName,
	}
	resp, err := tunnelClient.DescribeTunnel(req)
	if err != nil {
		log.Fatal("create test tunnel failed", err)
	}
	log.Println("tunnel id is", resp.Tunnel.TunnelId)

	//start consume tunnel
	workConfig := &tunnel.TunnelWorkerConfig{
		ProcessorFactory: &tunnel.AsyncProcessFactory{
			CustomValue:     "user defined interface{} value",
			ProcessFunc:     exampleConsumeFunction,
			ChannelOpenFunc: channelOpenCtx,
			ShutdownFunc: func(ctx *tunnel.ChannelContext) {
				fmt.Println("shutdown hook")
			},
			NeedBinaryRecord:     true,
			SyncCloseResource:    false,
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

func exampleConsumeFunction(ctx *tunnel.ChannelContext, records []*tunnel.Record) error {
	fmt.Println("user-defined information", ctx.CustomValue)
	fmt.Println("binaryRecord: ", ctx.BinaryRecords)
	fmt.Println("a round of records consumption finished")
	if ctx.NextToken == "finished" {
		ctx.Processor.CommitToken(ctx.NextToken)
	}
	return nil
}

func channelOpenCtx(ctx *tunnel.ChannelContext) error {
	fmt.Println("channelId: ", ctx.ChannelId)
	return nil
}
