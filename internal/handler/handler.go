package handler

import (
	"encoding/json"
	"path"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/vjeantet/jodaTime"

	"gl.weeget.cn/devops/sls2oss/internal"
	"gl.weeget.cn/devops/sls2oss/internal/filter"
	"gl.weeget.cn/devops/sls2oss/internal/metrics"
	"gl.weeget.cn/devops/sls2oss/internal/writer"
)

type ConsumeFunc func(map[string]interface{}) error

type MessageHandler struct {
	format  string
	filters []filter.FilterFunc
	Consume ConsumeFunc
	w       *writer.OssWriter
}

func New(logger log.Logger, format string, workerNum int, w *writer.OssWriter, quit <-chan struct{}) *MessageHandler {
	mh := &MessageHandler{
		format:  format,
		filters: make([]filter.FilterFunc, 0),
		w:       w,
	}
	if workerNum < 1 {
		// fallback to default 1
		workerNum = 1
	}
	if workerNum == 1 {
		mh.Consume = mh.consume
	} else {
		incoming := make(chan map[string]interface{}, workerNum)
		for i := 0; i < workerNum; i++ {
			w := newWorker(logger, mh.consume, incoming, quit)
			go w.loop()
		}
		mh.Consume = func(m map[string]interface{}) error {
			incoming <- m
			return nil
		}
	}
	return mh
}

func (mh *MessageHandler) AddFilters(filters ...filter.FilterFunc) {
	mh.filters = append(mh.filters, filters...)
}

func (mh *MessageHandler) consume(msg map[string]interface{}) (err error) {
	topic, ok := msg[internal.TopicKey].(string)
	if !ok {
		// skip msg without topic
		return nil
	}
	ts, ok := msg[internal.TimeKey].(time.Time)
	if !ok {
		// skip, same reason as topic
		return nil
	}
	metrics.PipelineEventInTotal.WithLabelValues(topic).Inc()

	for _, filter := range mh.filters {
		msg = filter(msg)
		if msg == nil {
			return nil
		}
	}

	writePath := path.Join(topic, mh.getPathFromTimestamp(ts))
	// todo: remove unnecessary fields
	b, err := json.Marshal(&msg)
	if err != nil {
		return err
	}
	b = append(b, []byte("\n")...)
	n, err := mh.w.WriteTo(writePath, b)
	if err != nil {
		return err
	}
	metrics.PipelineEventOutTotal.WithLabelValues(topic).Inc()
	metrics.PipelineWriteBytesTotal.WithLabelValues(topic, "temp", "plaintext").Add(float64(n))
	return nil
}

func (mh *MessageHandler) getPathFromTimestamp(t time.Time) string {
	return jodaTime.Format(mh.format, t)
}
