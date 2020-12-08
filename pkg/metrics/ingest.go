package metrics

import (
	"fmt"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/tidwall/gjson"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/hikhvar/mqtt2prometheus/pkg/config"
	"github.com/prometheus/client_golang/prometheus"
)

type Ingest struct {
	metricConfigs map[string][]*config.MetricConfig
	deviceIDRegex *config.Regexp
	collector     Collector
	MessageMetric *prometheus.CounterVec
	logger        *zap.Logger
}

func NewIngest(collector Collector, metrics []*config.MetricConfig, deviceIDRegex *config.Regexp) *Ingest {
	cfgs := make(map[string][]*config.MetricConfig)
	for i := range metrics {
		key := metrics[i].MQTTName
		cfgs[key] = append(cfgs[key], metrics[i])
	}
	return &Ingest{
		metricConfigs: cfgs,
		deviceIDRegex: deviceIDRegex,
		collector:     collector,
		MessageMetric: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "received_messages",
				Help: "received messages per topic and status",
			}, []string{"status", "topic"},
		),
		logger: config.ProcessContext.Logger(),
	}
}

// validMetric returns config matching the metric and deviceID
// Second return value indicates if config was found.
func (i *Ingest) validMetric(metric string, deviceID string) *config.MetricConfig {
	for _, c := range i.metricConfigs[metric] {
		if c.SensorNameFilter.Match(deviceID) {
			return c
		}
	}
	return nil
}

func (i *Ingest) store(topic string, payload []byte) error {

	var mc MetricCollection
	var metricValue float64

	deviceID := i.deviceID(topic)

	// Check if this metric already exists so we can update it.
	mci, found := i.collector.(*MemoryCachedCollector).cache.Get(deviceID)
	if found {
		mc = mci.(MetricCollection)
	} else {
		mc = make(MetricCollection, 0)
	}

	json := string(payload)
	for path := range i.metricConfigs {

		cfg := i.validMetric(path, deviceID)
		if cfg == nil || !cfg.SensorNameFilter.Match(deviceID) {
			i.logger.Debug("Invalid metric", zap.String("path", path), zap.String("deviceID", deviceID))
			continue
		}

		result := gjson.Get(json, path)
		switch result.Type {
		default:
			fallthrough
		case gjson.Null:
			i.logger.Debug("Invalid path", zap.String("path", path), zap.String("payload", string(payload)))
			continue
		case gjson.True, gjson.False:
			if result.Bool() {
				metricValue = 1
			} else {
				metricValue = 0
			}
		case gjson.String:
			strValue := result.String()
			// If string value mapping is defined, use that
			if cfg.StringValueMapping != nil {

				floatValue, ok := cfg.StringValueMapping.Map[strValue]
				if ok {
					metricValue = floatValue
				} else if cfg.StringValueMapping.ErrorValue != nil {
					metricValue = *cfg.StringValueMapping.ErrorValue
				} else {
					i.logger.Debug("got unexpected string data", zap.String("value", strValue))
					continue
				}

			} else {
				// otherwise try to parse float
				floatValue, err := strconv.ParseFloat(strValue, 64)
				if err != nil {
					i.logger.Debug("got data with unexpected type", zap.String("value", result.String()))
					continue
				}
				metricValue = floatValue
			}
		case gjson.Number:
			metricValue = result.Float()
		}

		keys := make([]string, 0)
		labels := make([]string, 0)
		for _, pL := range cfg.PromLabels {
			if mL, ok := cfg.VariableLabels[pL]; ok {
				result := gjson.Get(json, mL)
				if result.Type == gjson.Null {
					continue
				}
				keys = append(keys, pL)
				labels = append(labels, result.String())
			} else if pL == "topic" {
				keys = append(keys, pL)
				labels = append(labels, topic)
			} else if pL == "sensor" {
				keys = append(keys, pL)
				labels = append(labels, deviceID)
			}
		}

		metric := Metric{
			Description: cfg.PrometheusDescription(keys),
			Value:       metricValue,
			ValueType:   cfg.PrometheusValueType(),
			IngestTime:  time.Now(),
			Labels:      labels,
		}

		mc[path] = metric
	}

	i.collector.Observe(deviceID, mc)
	return nil
}

func (i *Ingest) SetupSubscriptionHandler(errChan chan<- error) mqtt.MessageHandler {
	return func(c mqtt.Client, m mqtt.Message) {
		i.logger.Debug("Got message", zap.String("topic", m.Topic()), zap.String("payload", string(m.Payload())))
		err := i.store(m.Topic(), m.Payload())
		if err != nil {
			errChan <- fmt.Errorf("could not store metrics '%s' on topic %s: %s", string(m.Payload()), m.Topic(), err.Error())
			i.MessageMetric.WithLabelValues("storeError", m.Topic()).Inc()
			return
		}
		i.MessageMetric.WithLabelValues("success", m.Topic()).Inc()
	}
}

// deviceID uses the configured DeviceIDRegex to extract the device ID from the given mqtt topic path.
func (i *Ingest) deviceID(topic string) string {
	return i.deviceIDRegex.GroupValue(topic, config.DeviceIDRegexGroup)
}
