// Copyright 2026 Ayesh Almeida
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	websubhub "github.com/ayeshLK/lib-websubhub"
)

type commandOptions struct {
	address          string
	path             string
	hubURL           string
	brokers          string
	eventsTopic      string
	updatesTopic     string
	deadLetterTopic  string
	deliveryGroup    string
	deliveryAttempts int
	retryBackoff     time.Duration
	startupTimeout   time.Duration
}

func main() {
	if err := run(); err != nil {
		slog.Error("sample Kafka hub stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	options := commandOptions{}
	flag.StringVar(&options.address, "addr", ":9090", "HTTP listen address")
	flag.StringVar(&options.path, "path", "/hub", "hub HTTP path")
	flag.StringVar(&options.hubURL, "hub-url", "http://localhost:9090/hub", "public absolute hub URL used in verification and delivery")
	flag.StringVar(&options.brokers, "brokers", "localhost:9092", "comma-separated Kafka bootstrap brokers")
	flag.StringVar(&options.eventsTopic, "events-topic", "websub-events", "single-partition Kafka topic containing application state events")
	flag.StringVar(&options.updatesTopic, "updates-topic", "websubhub-updates", "Kafka topic containing durable content updates")
	flag.StringVar(&options.deadLetterTopic, "dead-letter-topic", "websubhub-dead-letter", "Kafka topic receiving exhausted content updates")
	flag.StringVar(&options.deliveryGroup, "delivery-group", "websubhub-delivery", "Kafka consumer group used by delivery workers")
	flag.IntVar(&options.deliveryAttempts, "delivery-attempts", defaultDeliveryAttempts, "bounded delivery attempts before dead-lettering")
	flag.DurationVar(&options.retryBackoff, "retry-backoff", defaultRetryBackoff, "delay between delivery attempts")
	flag.DurationVar(&options.startupTimeout, "startup-timeout", 30*time.Second, "Kafka connection and event replay timeout")
	flag.Parse()
	if err := validateCommandOptions(options); err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	startupContext, cancelStartup := context.WithTimeout(context.Background(), options.startupTimeout)
	defer cancelStartup()
	broker, err := newKafkaBroker(startupContext, kafkaBrokerOptions{
		brokers:         splitBrokers(options.brokers),
		eventsTopic:     options.eventsTopic,
		updatesTopic:    options.updatesTopic,
		deadLetterTopic: options.deadLetterTopic,
		deliveryGroup:   options.deliveryGroup,
	})
	if err != nil {
		return err
	}
	application, err := newKafkaHub(startupContext, hubOptions{
		hubURL:           options.hubURL,
		broker:           broker,
		deliveryAttempts: options.deliveryAttempts,
		retryBackoff:     options.retryBackoff,
		logger:           logger,
	})
	if err != nil {
		return err
	}
	handler, err := websubhub.NewHandler(websubhub.Config{
		HubURL:                   options.hubURL,
		EnablePublisherExtension: true,
		Logger:                   logger,
	}, application.service())
	if err != nil {
		closeContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = application.Close(closeContext)
		return err
	}

	mux := http.NewServeMux()
	mux.Handle(options.path, handler)
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	})
	server := &http.Server{
		Addr:              options.address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	rootContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serverResult := make(chan error, 1)
	go func() {
		logger.Info("sample Kafka WebSub hub listening", "address", options.address, "path", options.path, "hub_url", options.hubURL)
		serverResult <- server.ListenAndServe()
	}()

	var serveErr error
	select {
	case <-rootContext.Done():
	case serveErr = <-serverResult:
	case serveErr = <-application.errorsChannel():
	}

	shutdownContext, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	shutdownErr := server.Shutdown(shutdownContext)
	handlerErr := handler.Close(shutdownContext)
	applicationErr := application.Close(shutdownContext)
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return errors.Join(serveErr, shutdownErr, handlerErr, applicationErr)
	}
	return errors.Join(shutdownErr, handlerErr, applicationErr)
}

func validateCommandOptions(options commandOptions) error {
	if options.address == "" {
		return errors.New("-addr must not be empty")
	}
	if !strings.HasPrefix(options.path, "/") {
		return errors.New("-path must start with /")
	}
	parsed, err := url.Parse(options.hubURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return errors.New("-hub-url must be an absolute HTTP(S) URL")
	}
	if parsed.Path != options.path {
		return fmt.Errorf("-hub-url path %q must match -path %q", parsed.Path, options.path)
	}
	if len(splitBrokers(options.brokers)) == 0 {
		return errors.New("-brokers must contain at least one address")
	}
	if options.eventsTopic == "" || options.updatesTopic == "" || options.deadLetterTopic == "" {
		return errors.New("Kafka topic names must not be empty")
	}
	if options.deliveryGroup == "" {
		return errors.New("-delivery-group must not be empty")
	}
	if options.deliveryAttempts <= 0 {
		return errors.New("-delivery-attempts must be greater than zero")
	}
	if options.retryBackoff < 0 {
		return errors.New("-retry-backoff must not be negative")
	}
	if options.startupTimeout <= 0 {
		return errors.New("-startup-timeout must be greater than zero")
	}
	return nil
}

func splitBrokers(value string) []string {
	parts := strings.Split(value, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}
