package consumer

import (
	"time"

	sls "github.com/aliyun/aliyun-log-go-sdk"
	consumerLibrary "github.com/aliyun/aliyun-log-go-sdk/consumer"
	"github.com/go-kit/kit/log/level"

	"github.com/fengxsong/sls2oss/internal"
)

type Consumer interface {
	Run(<-chan struct{}) error
}

type slsConsumer struct {
	config      *consumerLibrary.LogHubConfig
	cw          *consumerLibrary.ConsumerWorker
	consumeOne  func(map[string]interface{}) error
	includeMeta bool
}

func New(cfg *consumerLibrary.LogHubConfig, includeMeta bool, fn func(map[string]interface{}) error) Consumer {
	return &slsConsumer{
		config:      cfg,
		consumeOne:  fn,
		includeMeta: includeMeta,
	}
}

func (c *slsConsumer) Run(quit <-chan struct{}) error {
	c.cw = consumerLibrary.InitConsumerWorker(*c.config, c.process)
	c.cw.Start()
	<-quit
	level.Info(c.cw.Logger).Log("msg", "quiting")
	c.cw.StopAndWait()
	return nil
}

func (c *slsConsumer) process(shardId int, logGroupList *sls.LogGroupList) string {
	for _, lg := range logGroupList.LogGroups {
		for _, log := range lg.Logs {
			m := make(map[string]interface{})
			m[internal.TopicKey] = lg.GetCategory()
			m[internal.TimeKey] = time.Unix(int64(log.GetTime()), 0)
			if c.includeMeta {
				for i := range lg.LogTags {
					m[lg.LogTags[i].GetKey()] = lg.LogTags[i].GetValue()
				}
			}
			for _, content := range log.Contents {
				m[content.GetKey()] = content.GetValue()
			}
			if err := c.consumeOne(m); err != nil {
				level.Error(c.cw.Logger).Log("msg", "consume msg", "err", err)
			}
		}
	}
	return ""
}
