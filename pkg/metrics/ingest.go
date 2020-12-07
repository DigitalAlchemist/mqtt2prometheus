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
	metricConfigs map[string][]config.MetricConfig
	deviceIDRegex *config.Regexp
	collector     Collector
	MessageMetric *prometheus.CounterVec
	logger        *zap.Logger
}

func NewIngest(collector Collector, metrics []config.MetricConfig, deviceIDRegex *config.Regexp) *Ingest {
	cfgs := make(map[string][]config.MetricConfig)
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
func (i *Ingest) validMetric(metric string, deviceID string) (config.MetricConfig, bool) {
	for _, c := range i.metricConfigs[metric] {
		if c.SensorNameFilter.Match(deviceID) {
			return c, true
		}
	}
	return config.MetricConfig{}, false
}

func (i *Ingest) store(topic string, payload []byte) error {
	var mc MetricCollection
	deviceID := i.deviceID(topic)
	mci, found := i.collector.(*MemoryCachedCollector).cache.Get(deviceID)
	if found {
		mc = mci.(MetricCollection)
	} else {
		mc = make(MetricCollection, 0)
	}

	json := string(payload)

	for path := range i.metricConfigs {
		result := gjson.Get(json, path)
		if result.Type == gjson.Null {
			i.logger.Debug("Invalid path", zap.String("path", path), zap.String("payload", string(payload)))
			continue
		}
		m, err := i.parseMetric(path, deviceID, &result)
		if err != nil {
			i.logger.Debug("Error parsing metric", zap.String("err", err.Error()))
			return fmt.Errorf("failed to parse valid metric value: %w", err)
		}
		m.Topic = topic
		mc[path] = m
	}

	i.collector.Observe(deviceID, mc)
	return nil
}

func (i *Ingest) parseMetric(metricPath string, deviceID string, value *gjson.Result) (Metric, error) {
	cfg, cfgFound := i.validMetric(metricPath, deviceID)
	if !cfgFound {
		return Metric{}, nil
	}

	var metricValue float64

	switch value.Type {
	case gjson.True, gjson.False:
		if value.Bool() {
			metricValue = 1
		} else {
			metricValue = 0
		}
	case gjson.String:
		strValue := value.String()
		// If string value mapping is defined, use that
		if cfg.StringValueMapping != nil {

			floatValue, ok := cfg.StringValueMapping.Map[strValue]
			if ok {
				metricValue = floatValue
			} else if cfg.StringValueMapping.ErrorValue != nil {
				metricValue = *cfg.StringValueMapping.ErrorValue
			} else {
				return Metric{}, fmt.Errorf("got unexpected string data '%s'", strValue)
			}

		} else {

			// otherwise try to parse float
			floatValue, err := strconv.ParseFloat(strValue, 64)
			if err != nil {
				return Metric{}, fmt.Errorf("got data with unexpectd type: %T ('%s') and failed to parse to float", value, value)
			}
			metricValue = floatValue
		}
	case gjson.Number:
		metricValue = value.Float()
	default:
		return Metric{}, fmt.Errorf("got data with unexpectd type: %T ('%s')", value, value)
	}

	i.logger.Debug("Storing metric", zap.String("path", metricPath), zap.String("deviceID", deviceID), zap.Float64("value", metricValue))

	return Metric{
		Description: cfg.PrometheusDescription(),
		Value:       metricValue,
		ValueType:   cfg.PrometheusValueType(),
		IngestTime:  time.Now(),
	}, nil
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
