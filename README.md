# Aliyun tablestore SDK for Go

[![GitHub version](https://badge.fury.io/gh/aliyun%2Faliyun-tablestore-go-sdk.svg)](https://badge.fury.io/gh/aliyun%2Faliyun-tablestore-go-sdk)
[![Build Status](https://travis-ci.org/aliyun/aliyun-tablestore-go-sdk.svg?branch=master)](https://travis-ci.org/aliyun/aliyun-tablestore-go-sdk)
[![Coverage Status](https://coveralls.io/repos/github/aliyun/aliyun-tablestore-go-sdk/badge.svg?branch=master)](https://coveralls.io/github/aliyun/aliyun-tablestore-go-sdk?branch=master)

## 关于
> - 此Go SDK基于阿里云表格存储服务 API构建。
> - 阿里云表格存储是阿里云自主研发的NoSQL数据存储服务，提供海量结构化数据的存储和实时访问。

## 运行环境
> - 推荐使用Go 1.4及以上。

## 安装方法
### GitHub安装
> - 执行命令`go get github.com/aliyun/aliyun-tablestore-go-sdk`获取远程代码包。
> - 在您的代码中使用`import "github.com/aliyun/aliyun-tablestore-go-sdk"`引入TableStore Go SDK的包。

## HTTP代理
通过固定代理地址创建客户端：

```go
config := tablestore.NewDefaultTableStoreConfig()
config.ProxyHost = "http://user:password@proxy.example.com:8080"
client := tablestore.NewClientWithConfig(endpoint, instanceName, accessKeyID, accessKeySecret, securityToken, config)
```

设置`ProxyFromEnvironment`可读取`HTTP_PROXY`、`HTTPS_PROXY`和`NO_PROXY`环境变量。`ProxyHost`与环境代理同时设置时，`ProxyHost`优先；设置`Transport`时，代理行为完全由自定义Transport控制。

## 贡献代码
 - 我们非常欢迎大家为TableStore Go# SDK以及其他阿里云SDK贡献代码

## 联系我们
- [阿里云TableStore官方网站](http://www.aliyun.com/product/ots)
- [阿里云TableStore官方论坛](http://bbs.aliyun.com)
- [阿里云TableStore官方文档中心](https://help.aliyun.com/product/8315004_ots.html)
- [阿里云云栖社区](http://yq.aliyun.com)
- [阿里云工单系统](https://workorder.console.aliyun.com/#/ticket/createIndex)

### 扫码加入TableStore讨论群，和我们直接交流讨论
钉钉群号：23307953

## 运行测试
1. 修改`testConfig/config.go`中的配置信息，包括访问阿里云TableStore服务的地址、访问凭证等。
2. 运行单个suite：```go test -test.v -timeout 60m github.com/aliyun/aliyun-tablestore-go-sdk/tablestore -check.f TableStoreSuite```
