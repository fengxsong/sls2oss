package handler

import (
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
)

type worker struct {
	logger   log.Logger
	consume  ConsumeFunc
	incoming chan map[string]interface{}
	quit     <-chan struct{}
}

func newWorker(logger log.Logger, consume ConsumeFunc, incoming chan map[string]interface{}, quit <-chan struct{}) *worker {
	return &worker{
		logger:   logger,
		consume:  consume,
		incoming: incoming,
		quit:     quit,
	}
}

func (w *worker) loop() {
	for {
		select {
		case m, ok := <-w.incoming:
			if !ok {
				return
			}
			if err := w.consume(m); err != nil {
				level.Error(w.logger).Log("msg", "consuming", "err", err)
			}
		case <-w.quit:
			return
		}
	}
}
