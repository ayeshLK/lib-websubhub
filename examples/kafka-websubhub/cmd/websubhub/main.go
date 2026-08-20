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
	"github.com/ayeshLK/lib-websubhub/examples/kafka-websubhub/internal/config"
	kafkahub "github.com/ayeshLK/lib-websubhub/examples/kafka-websubhub/websubhub"
)

func main() {
	if err := run(); err != nil {
		slog.Error("sample Kafka hub stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	options, err := config.LoadWebSubHub()
	if err != nil {
		return err
	}
	if err := validateConfig(options); err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	startupContext, cancelStartup := context.WithTimeout(context.Background(), options.StartupTimeout.Duration)
	defer cancelStartup()
	broker, err := kafkahub.NewKafkaBroker(startupContext, kafkahub.KafkaBrokerOptions{
		Brokers:     options.Brokers,
		EventsTopic: options.EventsTopic,
	})
	if err != nil {
		return err
	}
	snapshotSource, err := kafkahub.NewHTTPSnapshotSource(options.ConsolidatorURL, nil)
	if err != nil {
		broker.Close()
		return err
	}
	application, err := kafkahub.New(startupContext, kafkahub.Options{
		HubURL:           options.HubURL,
		ServerID:         options.ServerID,
		Broker:           broker,
		SnapshotSource:   snapshotSource,
		DeliveryAttempts: options.DeliveryAttempts,
		RetryBackoff:     options.RetryBackoff.Duration,
		Logger:           logger,
	})
	if err != nil {
		return err
	}
	handler, err := websubhub.NewHandler(websubhub.Config{
		HubURL:                   options.HubURL,
		EnablePublisherExtension: true,
		Logger:                   logger,
	}, application.Service())
	if err != nil {
		closeContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = application.Close(closeContext)
		return err
	}

	mux := http.NewServeMux()
	mux.Handle(options.Path, handler)
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	})
	server := &http.Server{
		Addr:              options.Address,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	rootContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serverResult := make(chan error, 1)
	go func() {
		logger.Info("sample Kafka WebSub hub listening", "address", options.Address, "path", options.Path, "hub_url", options.HubURL)
		serverResult <- server.ListenAndServe()
	}()

	var serveErr error
	select {
	case <-rootContext.Done():
	case serveErr = <-serverResult:
	case serveErr = <-application.Errors():
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

func validateConfig(options config.WebSubHub) error {
	if strings.TrimSpace(options.ServerID) == "" {
		return errors.New("websubhub.server_id must not be empty")
	}
	if options.Address == "" {
		return errors.New("websubhub.address must not be empty")
	}
	if !strings.HasPrefix(options.Path, "/") {
		return errors.New("websubhub.path must start with /")
	}
	parsed, err := url.Parse(options.HubURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return errors.New("websubhub.hub_url must be an absolute HTTP(S) URL")
	}
	if parsed.Path != options.Path {
		return fmt.Errorf("websubhub.hub_url path %q must match websubhub.path %q", parsed.Path, options.Path)
	}
	if len(options.Brokers) == 0 {
		return errors.New("websubhub.brokers must contain at least one address")
	}
	for _, broker := range options.Brokers {
		if strings.TrimSpace(broker) == "" {
			return errors.New("websubhub.brokers must not contain an empty address")
		}
	}
	if options.EventsTopic == "" {
		return errors.New("websubhub.events_topic must not be empty")
	}
	consolidatorURL, err := url.Parse(options.ConsolidatorURL)
	if err != nil || (consolidatorURL.Scheme != "http" && consolidatorURL.Scheme != "https") || consolidatorURL.Host == "" {
		return errors.New("websubhub.consolidator_url must be an absolute HTTP(S) URL")
	}
	if options.DeliveryAttempts <= 0 {
		return errors.New("websubhub.delivery_attempts must be greater than zero")
	}
	if options.RetryBackoff.Duration < 0 {
		return errors.New("websubhub.retry_backoff must not be negative")
	}
	if options.StartupTimeout.Duration <= 0 {
		return errors.New("websubhub.startup_timeout must be greater than zero")
	}
	return nil
}
