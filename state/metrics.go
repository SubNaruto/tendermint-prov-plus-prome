package state

import (
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/discard"
	"github.com/go-kit/kit/metrics/prometheus"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
)

const (
	// MetricsSubsystem is a subsystem shared by all metrics exposed by this
	// package.
	MetricsSubsystem = "state"
)

// Metrics contains metrics exposed by this package.
type Metrics struct {
	// Time between BeginBlock and EndBlock.
	BlockProcessingTime metrics.Histogram
	//// Number of processed queries.
	//PcdQrs metrics.Gauge
	//// Total number of queries.
	//TotalQrs metrics.Gauge
	//// The latest second qps.
	//AvgQps metrics.Gauge
}

// PrometheusMetrics returns Metrics build using Prometheus client library.
// Optionally, labels can be provided along with their values ("foo",
// "fooValue").
func PrometheusMetrics(namespace string, labelsAndValues ...string) *Metrics {
	labels := []string{}
	for i := 0; i < len(labelsAndValues); i += 2 {
		labels = append(labels, labelsAndValues[i])
	}
	return &Metrics{
		BlockProcessingTime: prometheus.NewHistogramFrom(stdprometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: MetricsSubsystem,
			Name:      "block_processing_time",
			Help:      "Time between BeginBlock and EndBlock in ms.",
			Buckets:   stdprometheus.LinearBuckets(1, 10, 10),
		}, labels).With(labelsAndValues...),
		//PcdQrs: prometheus.NewGaugeFrom(stdprometheus.GaugeOpts{
		//	Namespace: namespace,
		//	Subsystem: MetricsSubsystem,
		//	Name:      "num_qrs",
		//	Help:      "Number of processed queries.",
		//}, labels).With(labelsAndValues...),
		//TotalQrs: prometheus.NewGaugeFrom(stdprometheus.GaugeOpts{
		//	Namespace: namespace,
		//	Subsystem: MetricsSubsystem,
		//	Name:      "total_qrs",
		//	Help:      "Total number of queries.",
		//}, labels).With(labelsAndValues...),
		//AvgQps: prometheus.NewGaugeFrom(stdprometheus.GaugeOpts{
		//	Namespace: namespace,
		//	Subsystem: MetricsSubsystem,
		//	Name:      "latest_average_qps",
		//	Help:      "The latest second qps.",
		//}, labels).With(labelsAndValues...),
	}
}

// NopMetrics returns no-op Metrics.
func NopMetrics() *Metrics {
	return &Metrics{
		BlockProcessingTime: discard.NewHistogram(),
		//PcdQrs:              discard.NewGauge(),
		//TotalQrs:            discard.NewGauge(),
		//AvgQps:              discard.NewGauge(),
	}
}
