package metrics

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const namespace = "sls2oss"

var (
	PipelineEventInTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "pipeline",
			Name:      "event_in_total",
			Help:      "total pipeline events in",
		}, []string{"logstore"},
	)
	PipelineEventOutTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "pipeline",
			Name:      "event_out_total",
			Help:      "total pipeline events out",
		}, []string{"logstore"},
	)
	PipelineWriteBytesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "pipeline",
			Name:      "write_bytes_total",
			Help:      "total bytes write out",
		}, []string{"logstore", "to", "type"},
	)
)

func init() {
	prometheus.MustRegister(PipelineEventInTotal, PipelineEventOutTotal, PipelineWriteBytesTotal)
}

func Serve(port int, metricPath string, logger log.Logger, quit <-chan struct{}) error {
	if port > 0 {
		if metricPath == "" {
			metricPath = "/metrics"
		}
		http.Handle(metricPath, promhttp.Handler())
		level.Info(logger).Log("msg", "start prometheus metrics handler", "port", port, "path", metricPath)
		errch := make(chan error, 1)
		go func() {
			errch <- http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
		}()
		select {
		case err := <-errch:
			return err
		case <-quit:
			return nil
		}
	}
	return nil
}
