package metrics

import (
	"time"

	"github.com/hikhvar/mqtt2prometheus/pkg/config"
	gocache "github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus"
)

const DefaultTimeout = 0

type Collector interface {
	prometheus.Collector
	Observe(deviceID string, collection MetricCollection)
}

type MemoryCachedCollector struct {
	cache        *gocache.Cache
	descriptions []*prometheus.Desc
}

type Metric struct {
	Description *prometheus.Desc
	Value       float64
	ValueType   prometheus.ValueType
	IngestTime  time.Time
	Labels      []string
}

type MetricCollection map[string]Metric

func NewCollector(defaultTimeout time.Duration, possibleMetrics []*config.MetricConfig) Collector {
	return &MemoryCachedCollector{
		cache: gocache.New(defaultTimeout, defaultTimeout*10),
	}
}

func (c *MemoryCachedCollector) Observe(deviceID string, collection MetricCollection) {
	c.cache.Set(deviceID, collection, DefaultTimeout)
}

func (c *MemoryCachedCollector) Describe(ch chan<- *prometheus.Desc) {}

func (c *MemoryCachedCollector) Collect(mc chan<- prometheus.Metric) {
	for _, metricsRaw := range c.cache.Items() {
		metrics := metricsRaw.Object.(MetricCollection)
		for _, metric := range metrics {
			m := prometheus.MustNewConstMetric(
				metric.Description,
				metric.ValueType,
				metric.Value,
				metric.Labels...,
			)
			mc <- prometheus.NewMetricWithTimestamp(metric.IngestTime, m)
		}
	}
}
