package tunnel

import (
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"net/http"
	"time"
)

var (
	DefaultHeartbeatInterval = 30 * time.Second
	DefaultHeartbeatTimeout  = 300 * time.Second
)

var DefaultTunnelConfig = &TunnelConfig{
	MaxRetryElapsedTime: 75 * time.Second,
	RequestTimeout:      60 * time.Second,
	Transport:           http.DefaultTransport,
}

var DefaultLogConfig = zap.Config{
	Level:       zap.NewAtomicLevelAt(zap.InfoLevel),
	Development: false,
	Sampling: &zap.SamplingConfig{
		Initial:    100,
		Thereafter: 100,
	},
	Encoding: "json",
	EncoderConfig: zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	},
}

var DefaultSyncer = zapcore.AddSync(&lumberjack.Logger{
	Filename:   "tunnelClient.log",
	MaxSize:    512, //MB
	MaxBackups: 5,
	MaxAge:     30, //days
	Compress:   true,
})

type ChannelContext struct {
	TunnelId  string
	ClientId  string
	ChannelId string

	TraceId string

	NextToken string

	CustomValue interface{}
}

func (c *ChannelContext) String() string {
	return fmt.Sprintf("TunnelId %s, ClientId %s, ChannelId %s", c.TunnelId, c.ClientId, c.ChannelId)
}

func newChannelContext(tunnelId, clientId, channelId string, customValue interface{}) *ChannelContext {
	return &ChannelContext{TunnelId: tunnelId, ChannelId: channelId, ClientId: clientId, CustomValue: customValue}
}

type TunnelWorkerConfig struct {
	HeartbeatTimeout  time.Duration
	HeartbeatInterval time.Duration
	ChannelDialer     ChannelDialer

	ProcessorFactory ChannelProcessorFactory

	LogConfig      *zap.Config
	LogWriteSyncer zapcore.WriteSyncer
}

// hack replace zap config build core with lumberjack logger
func ReplaceLogCore(ws zapcore.WriteSyncer, conf zap.Config) zap.Option {
	var enc zapcore.Encoder
	// Copy paste from zap.Config.buildEncoder.
	switch conf.Encoding {
	case "json":
		enc = zapcore.NewJSONEncoder(conf.EncoderConfig)
	case "console":
		enc = zapcore.NewConsoleEncoder(conf.EncoderConfig)
	default:
		panic("unknown encoding")
	}
	return zap.WrapCore(func(zapcore.Core) zapcore.Core {
		return zapcore.NewCore(enc, ws, conf.Level)
	})
}

type TunnelConfig struct {
	MaxRetryElapsedTime time.Duration
	RequestTimeout      time.Duration
	Transport           http.RoundTripper
}
