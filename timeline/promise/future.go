package promise

import (
	"errors"
	"sync"
)

type Future struct {
	done   chan struct{}
	result interface{}
	err    error
}

func NewFuture(task func() (interface{}, error)) *Future {
	f := &Future{
		done: make(chan struct{}),
	}
	go func() {
		defer close(f.done)
		f.result, f.err = task()
	}()
	return f
}

func (f *Future) Get() (interface{}, error) {
	<-f.done
	return f.result, f.err
}

func (f *Future) FanInGet() ([]*FanResult, error) {
	ret, err := f.Get()
	if err != nil {
		return nil, err
	}
	if fanRet, ok := ret.([]*FanResult); ok {
		return fanRet, nil
	}
	return nil, errors.New("not a fan in future")
}

type FanResult struct {
	Result interface{}
	Err    error
}

func FanIn(futures ...*Future) *Future {
	fanFuture := NewFuture(func() (interface{}, error) {
		fanResults := make([]*FanResult, len(futures))
		wg := new(sync.WaitGroup)
		wg.Add(len(futures))

		for i, f := range futures {
			go func(idx int) {
				defer wg.Done()
				ret, err := f.Get()
				fanResults[idx] = &FanResult{Result: ret, Err: err}
			}(i)
		}

		wg.Wait()
		return fanResults, nil
	})
	return fanFuture
}
