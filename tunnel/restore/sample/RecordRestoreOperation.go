package sample

import (
	"fmt"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tunnel"
	"github.com/aliyun/aliyun-tablestore-go-sdk/tunnel/restore"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var client *tablestore.TableStoreClient

func BinaryRecordRestoreSample(tunnelClient tunnel.TunnelClient, tableName string) {
	tunnelName := "exampleTunnel"
	req := &tunnel.DescribeTunnelRequest{
		TableName:  tableName,
		TunnelName: tunnelName,
	}
	resp, err := tunnelClient.DescribeTunnel(req)
	if err != nil {
		log.Fatal("describe tunnel failed", err)
	}
	log.Println("tunnel id is", resp.Tunnel.TunnelId)

	//start consume tunnel
	workConfig := &tunnel.TunnelWorkerConfig{
		ProcessorFactory: &tunnel.AsyncProcessFactory{
			CustomValue: "user defined interface{} value",
			ProcessFunc: exampleConsumeFunction1,
			ShutdownFunc: func(ctx *tunnel.ChannelContext) {
				fmt.Println("shutdown hook")
			},
			NeedBinaryRecord: true,
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

func exampleConsumeFunction1(ctx *tunnel.ChannelContext, records []*tunnel.Record) error {
	fmt.Println("user-defined information", ctx.CustomValue)
	request := &restore.BinaryRecordReplayRequest{
		Record:    ctx.BinaryRecords,
		TableName: "tableName",
	}
	_, err := restore.BinaryRecordRestore(client, request)
	if err != nil {
		return err
	}
	return nil
}

func RecordRestoreSample(tunnelClient tunnel.TunnelClient, tableName string) {
	tunnelName := "exampleTunnel"
	req := &tunnel.DescribeTunnelRequest{
		TableName:  tableName,
		TunnelName: tunnelName,
	}
	resp, err := tunnelClient.DescribeTunnel(req)
	if err != nil {
		log.Fatal("describe tunnel failed", err)
	}
	log.Println("tunnel id is", resp.Tunnel.TunnelId)

	//start consume tunnel
	workConfig := &tunnel.TunnelWorkerConfig{
		ProcessorFactory: &tunnel.AsyncProcessFactory{
			CustomValue: "user defined interface{} value",
			ProcessFunc: exampleConsumeFunction2,
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

func exampleConsumeFunction2(ctx *tunnel.ChannelContext, records []*tunnel.Record) error {
	fmt.Println("user-defined information", ctx.CustomValue)
	request := &restore.RecordReplayRequest{
		Record:    records,
		TableName: "tableName",
	}
	_, err := restore.RecordRestore(client, request)
	if err != nil {
		return err
	}
	return nil
}
