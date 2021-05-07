package tunnel

import (
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"sync"
	"time"
)

var (
	DefaultChannelSize        = 10
	DefaultCheckpointInterval = 10 * time.Second
)

type ChannelProcessorFactory interface {
	NewProcessor(tunnelId, clientId, channelId string, checkpointer Checkpointer) (ChannelProcessor, error)
}

type ChannelProcessor interface {
	Process(records []*Record, nextToken, traceId string) error
	Shutdown()
	Error() bool
	Finished() bool
}

type SimpleProcessFactory struct {
	CustomValue interface{}

	CpInterval time.Duration

	ProcessFunc  func(channelCtx *ChannelContext, records []*Record) error
	ShutdownFunc func(channelCtx *ChannelContext)

	Logger *zap.Logger
}

func (s *SimpleProcessFactory) NewProcessor(tunnelId, clientId, channelId string, checkpointer Checkpointer) (ChannelProcessor, error) {
	var lg *zap.Logger
	if s.Logger == nil {
		lg, _ = DefaultLogConfig.Build(ReplaceLogCore(DefaultSyncer, DefaultLogConfig))
	} else {
		lg = s.Logger
	}
	interval := DefaultCheckpointInterval
	if s.CpInterval > 0 {
		interval = s.CpInterval
	}
	p := &defaultProcessor{
		ctx:          NewChannelContext(tunnelId, clientId, channelId, s.CustomValue),
		checkpointer: checkpointer,
		processFunc:  s.ProcessFunc,
		shutdownFunc: s.ShutdownFunc,
		checkpointCh: make(chan string, DefaultChannelSize),
		closeCh:      make(chan struct{}),
		ticker:       time.NewTicker(interval),
		wg:           new(sync.WaitGroup),
		lg:           lg,
		finished:     atomic.NewBool(false),
	}
	p.wg.Add(1)
	go p.cpLoop()
	return p, nil
}

type defaultProcessor struct {
	ctx *ChannelContext

	checkpointer Checkpointer
	processFunc  func(channelCtx *ChannelContext, records []*Record) error
	shutdownFunc func(channelCtx *ChannelContext)

	checkpointCh chan string
	closeCh      chan struct{}
	closeOnce    sync.Once
	ticker       *time.Ticker
	wg           *sync.WaitGroup

	finished *atomic.Bool

	lg *zap.Logger
}

func (p *defaultProcessor) Process(records []*Record, nextToken, traceId string) error {
	if len(records) != 0 {
		ctx := &ChannelContext{
			TunnelId:    p.ctx.TunnelId,
			ClientId:    p.ctx.ClientId,
			ChannelId:   p.ctx.ChannelId,
			TraceId:     traceId,
			NextToken:   nextToken,
			CustomValue: p.ctx.CustomValue,
		}
		err := p.processFunc(ctx, records)
		if err != nil {
			return err
		}
	}
	select {
	case p.checkpointCh <- nextToken:
	case <-p.closeCh:
	}
	if nextToken == FinishTag {
		p.finished.Store(true)
		p.Shutdown()
	}
	return nil
}

func (p *defaultProcessor) Error() bool {
	return false
}

func (p *defaultProcessor) Shutdown() {
	p.closeOnce.Do(func() {
		close(p.closeCh)
		p.ticker.Stop()
		p.wg.Wait()
		if p.shutdownFunc != nil {
			ctx := &ChannelContext{
				TunnelId:    p.ctx.TunnelId,
				ClientId:    p.ctx.ClientId,
				ChannelId:   p.ctx.ChannelId,
				CustomValue: p.ctx.CustomValue,
			}
			p.shutdownFunc(ctx)
		}
	})
}

func (p *defaultProcessor) Finished() bool {
	return p.finished.Load()
}

func (p *defaultProcessor) cpLoop() {
	defer p.wg.Done()
	newCp := ""
	cpFlush := make(chan string)

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			cp, ok := <-cpFlush
			if !ok {
				return
			}
			err := p.checkpointer.Checkpoint(cp)
			if err != nil {
				p.lg.Error("make checkpoint failed", zap.String("checkpoint", cp), zap.Error(err))
			} else {
				p.lg.Info("checkpoint progress", zap.String("context", p.ctx.String()), zap.String("checkpoint", cp))
			}
		}
	}()
	for {
		select {
		case cp := <-p.checkpointCh:
			newCp = cp
		case <-p.ticker.C:
			if newCp != "" {
				select {
				case cpFlush <- newCp:
					newCp = ""
				default:
				}
			}
		case <-p.closeCh:
			if newCp != "" {
				cpFlush <- newCp
			}
			close(cpFlush)
			return
		}
	}
}
