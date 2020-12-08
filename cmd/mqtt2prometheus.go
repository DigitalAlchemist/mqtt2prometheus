package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/hikhvar/mqtt2prometheus/pkg/config"
	"github.com/hikhvar/mqtt2prometheus/pkg/metrics"
	"github.com/hikhvar/mqtt2prometheus/pkg/mqttclient"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// These variables are set by goreleaser at linking time.
var (
	version string
	commit  string
	date    string
)

var (
	configFlag = flag.String(
		"config",
		"config.yaml",
		"config file",
	)
	portFlag = flag.String(
		"listen-port",
		"9641",
		"HTTP port used to expose metrics",
	)
	addressFlag = flag.String(
		"listen-address",
		"0.0.0.0",
		"listen address for HTTP server used to expose metrics",
	)
	versionFlag = flag.Bool(
		"version",
		false,
		"show the builds version, date and commit",
	)
	logLevelFlag    = zap.LevelFlag("log-level", zap.InfoLevel, "sets the default loglevel (default: \"info\")")
	logEncodingFlag = flag.String(
		"log-format",
		"console",
		"set the desired log output format. Valid values are 'console' and 'json'",
	)
)

func main() {
	flag.Parse()
	if *versionFlag {
		mustShowVersion()
		os.Exit(0)
	}
	logger := mustSetupLogger()
	defer logger.Sync() //nolint:errcheck
	c := make(chan os.Signal, 1)
	hostName, err := os.Hostname()
	if err != nil {
		logger.Fatal("Could not get hostname", zap.Error(err))
	}
	cfg, err := config.LoadConfig(*configFlag)
	if err != nil {
		logger.Fatal("Could not load config", zap.Error(err))
	}
	mqttClientOptions := mqtt.NewClientOptions()
	mqttClientOptions.AddBroker(cfg.MQTT.Server).SetClientID(hostName).SetCleanSession(true)
	mqttClientOptions.SetUsername(cfg.MQTT.User)
	mqttClientOptions.SetPassword(cfg.MQTT.Password)
	mqttClientOptions.SetClientID(mustMQTTClientID())

	collector := metrics.NewCollector(cfg.Cache.Timeout, cfg.Metrics)
	ingest := metrics.NewIngest(collector, cfg.Metrics, cfg.MQTT.DeviceIDRegex)

	errorChan := make(chan error, 1)

	for {
		err = mqttclient.Subscribe(mqttClientOptions, mqttclient.SubscribeOptions{
			Topic:             cfg.MQTT.TopicPath,
			QoS:               cfg.MQTT.QoS,
			OnMessageReceived: ingest.SetupSubscriptionHandler(errorChan),
			Logger:            logger,
		})
		if err == nil {
			// connected, break loop
			break
		}
		logger.Warn("could not connect to mqtt broker %s, sleep 10 second", zap.Error(err))
		time.Sleep(10 * time.Second)
	}

	prometheus.MustRegister(ingest.MessageMetric)
	prometheus.MustRegister(collector)
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		err = http.ListenAndServe(getListenAddress(), nil)
		if err != nil {
			logger.Fatal("Error while serving http", zap.Error(err))
		}
	}()

	for {
		select {
		case <-c:
			logger.Info("Terminated via Signal. Stop.")
			os.Exit(0)
		case err = <-errorChan:
			logger.Error("Error while processing message", zap.Error(err))
		}
	}
}

func getListenAddress() string {
	return fmt.Sprintf("%s:%s", *addressFlag, *portFlag)
}

func mustShowVersion() {
	versionInfo := struct {
		Version string
		Commit  string
		Date    string
	}{
		Version: version,
		Commit:  commit,
		Date:    date,
	}

	err := json.NewEncoder(os.Stdout).Encode(versionInfo)
	if err != nil {
		panic(err)
	}
}

func mustMQTTClientID() string {
	host, err := os.Hostname()
	if err != nil {
		panic(fmt.Sprintf("failed to get hostname: %v", err))
	}
	pid := os.Getpid()
	return fmt.Sprintf("%s-%d", host, pid)
}

func mustSetupLogger() *zap.Logger {
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(*logLevelFlag)
	cfg.Encoding = *logEncodingFlag
	if cfg.Encoding == "console" {
		cfg.EncoderConfig.EncodeTime = zapcore.RFC3339TimeEncoder
	}
	logger, err := cfg.Build()
	if err != nil {
		panic(fmt.Sprintf("failed to build logger: %v", err))
	}

	config.SetProcessContext(logger)
	return logger
}
