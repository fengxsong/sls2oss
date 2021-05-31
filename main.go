package main

import (
	"fmt"
	"os"
	"strings"

	consumerLibrary "github.com/aliyun/aliyun-log-go-sdk/consumer"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/spf13/pflag"
	_ "go.uber.org/automaxprocs"
	"golang.org/x/sync/errgroup"

	"github.com/fengxsong/sls2oss/internal"
	"github.com/fengxsong/sls2oss/internal/config"
	"github.com/fengxsong/sls2oss/internal/consumer"
	"github.com/fengxsong/sls2oss/internal/handler"
	"github.com/fengxsong/sls2oss/internal/metrics"
	"github.com/fengxsong/sls2oss/internal/version"
	"github.com/fengxsong/sls2oss/internal/writer"
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

	cfg, err := config.ReadFromFile(configFileF)
	if err != nil {
		fatal("read config error", err)
	}

	if cfg.Logging.Level == "" || pflag.Lookup("log-level").Changed {
		cfg.Logging.Level = logLevelF
	}
	logger := initLogger(cfg.Logging)

	quit := internal.SetupSignalHandler()
	ossWriter, err := writer.NewOssWriter(cfg.Output.Oss, logger, quit)
	if err != nil {
		fatal("failed to create oss writer", err)
	}
	if cfg.Output.Oss.SyncOrphanedFiles {
		if err = ossWriter.StartWait(); err != nil {
			fatal("failed to do some prestart jobs", err)
		}
	}
	h := handler.New(logger, dateFmtF, cfg.Worker, ossWriter, quit)
	g := &errgroup.Group{}
	// wait for oss write to complete.
	g.Go(func() error { return ossWriter.Wait() })
	g.Go(func() error { return metrics.Serve(cfg.Metric.Port, cfg.Metric.Path, logger, quit) })
	for _, ls := range cfg.Input.Sls.Logstores {
		lsLogger := log.With(logger, "logstore", ls)
		consumer := consumer.New(toLogHubConfig(cfg.Input.Sls, ls), lsLogger, cfg.Input.Sls.IncludeMeta, h.Consume)
		g.Go(func() error { return consumer.Run(quit) })
	}
	if err := g.Wait(); err != nil {
		fatal("error occur while waiting goroutines to exit", err)
	}
}

func fatal(args ...interface{}) {
	fmt.Println(args...)
	os.Exit(1)
}

func initLogger(cfg *config.Logging) log.Logger {
	writer := os.Stdout
	if len(cfg.File) > 0 {
		var err error
		writer, err = os.OpenFile(cfg.File, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
	var logger log.Logger
	switch strings.ToLower(cfg.Format) {
	case "none":
		logger = log.NewNopLogger()
	case "json":
		logger = log.NewJSONLogger(writer)
	default:
		logger = log.NewLogfmtLogger(writer)
	}
	switch cfg.Level {
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
