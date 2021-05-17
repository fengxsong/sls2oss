package main

import (
	"fmt"
	"os"

	consumerLibrary "github.com/aliyun/aliyun-log-go-sdk/consumer"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/spf13/pflag"
	"golang.org/x/sync/errgroup"

	"gl.weeget.cn/devops/sls2oss/internal"
	"gl.weeget.cn/devops/sls2oss/internal/config"
	"gl.weeget.cn/devops/sls2oss/internal/consumer"
	"gl.weeget.cn/devops/sls2oss/internal/handler"
	"gl.weeget.cn/devops/sls2oss/internal/metrics"
	"gl.weeget.cn/devops/sls2oss/internal/version"
	"gl.weeget.cn/devops/sls2oss/internal/writer"
)

var (
	configFileF string
	dateFmtF    string
	logLevelF   string
)

func toLogHubConfig(c *config.SlsConfig, logstore string) *consumerLibrary.LogHubConfig {
	return &consumerLibrary.LogHubConfig{
		Endpoint:              c.Endpoint,
		AccessKeyID:           c.AccessKeyID,
		AccessKeySecret:       c.AccessKeySecret,
		Project:               c.Project,
		Logstore:              logstore,
		ConsumerGroupName:     c.ConsumerGroupName,
		ConsumerName:          c.ConsumerName,
		CursorPosition:        c.CursorPosition,
		CursorStartTime:       c.CursorStartTime,
		InOrder:               c.InOrder,
		MaxFetchLogGroupCount: c.MaxFetchLogGroupCount,
		DataFetchIntervalInMs: int64(c.DataFetchIntervalInMs),
	}
}

func main() {
	var printVersion bool
	pflag.StringVarP(&configFileF, "config", "c", "config.yaml", "JSON/YAML file of config")
	pflag.StringVar(&dateFmtF, "date-format", "yyyy/MM/dd/HH", "date format for dirs")
	pflag.StringVar(&logLevelF, "log-level", "info", "logging level")
	pflag.BoolVarP(&printVersion, "version", "v", false, "print build version info")
	pflag.Parse()

	if printVersion {
		fmt.Println(version.Version())
		return
	}

	logger := initLogger(logLevelF)
	cfg, err := config.ReadFromFile(configFileF)
	if err != nil {
		fatal(logger, "msg", "read config", "err", err)
	}
	if err = cfg.ValidateAndSetDefaults(); err != nil {
		fatal(logger, "msg", "invalid config", "err", err)
	}
	quit := internal.SetupSignalHandler()
	ossWriter, err := writer.NewOssWriter(cfg.Output.Oss, logger, quit)
	if err != nil {
		fatal(logger, "msg", "create oss writer", "err", err)
	}
	h := handler.New(logger, dateFmtF, cfg.Worker, ossWriter, quit)
	g := &errgroup.Group{}
	g.Go(func() error { return metrics.Serve(cfg.Metric.Port, cfg.Metric.Path, logger, quit) })
	for _, ls := range cfg.Input.Sls.Logstores {
		consumer := consumer.New(toLogHubConfig(cfg.Input.Sls, ls), cfg.Input.Sls.IncludeMeta, h.Consume)
		g.Go(func() error { return consumer.Run(quit) })
	}
	if err := g.Wait(); err != nil {
		fatal(logger, "msg", err)
	}
}

func fatal(logger log.Logger, keyvals ...interface{}) {
	level.Error(logger).Log(keyvals...)
	os.Exit(1)
}

func initLogger(lvl string) log.Logger {
	logger := log.NewLogfmtLogger(os.Stdout)
	switch lvl {
	case "debug":
		logger = level.NewFilter(logger, level.AllowDebug())
	case "warn":
		logger = level.NewFilter(logger, level.AllowWarn())
	case "error":
		logger = level.NewFilter(logger, level.AllowError())
	default:
		logger = level.NewFilter(logger, level.AllowInfo())
	}
	logger = log.With(logger, "time", log.DefaultTimestamp, "caller", log.DefaultCaller)
	return logger
}
