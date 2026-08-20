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
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ayeshLK/lib-websubhub/examples/kafka-websubhub/consolidator"
	"github.com/ayeshLK/lib-websubhub/examples/kafka-websubhub/internal/config"
)

func main() {
	if err := run(); err != nil {
		slog.Error("sample Kafka consolidator stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	options, err := config.LoadConsolidator()
	if err != nil {
		return err
	}
	if err := validateConfig(options); err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	startupContext, cancelStartup := context.WithTimeout(context.Background(), options.StartupTimeout.Duration)
	defer cancelStartup()
	broker, err := consolidator.NewKafkaBroker(startupContext, consolidator.KafkaBrokerOptions{
		Brokers:        options.Brokers,
		EventsTopic:    options.EventsTopic,
		SnapshotsTopic: options.SnapshotsTopic,
	})
	if err != nil {
		return err
	}
	application, err := consolidator.New(startupContext, consolidator.Options{
		Broker: broker,
		Logger: logger,
	})
	if err != nil {
		return err
	}

	server := &http.Server{
		Addr:              options.Address,
		Handler:           application.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	rootContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serverResult := make(chan error, 1)
	go func() {
		logger.Info("sample Kafka consolidator listening", "address", options.Address)
		serverResult <- server.ListenAndServe()
	}()

	var serveErr error
	select {
	case <-rootContext.Done():
	case serveErr = <-serverResult:
	case serveErr = <-application.Errors():
	}
	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelShutdown()
	shutdownErr := server.Shutdown(shutdownContext)
	applicationErr := application.Close(shutdownContext)
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return errors.Join(serveErr, shutdownErr, applicationErr)
	}
	return errors.Join(shutdownErr, applicationErr)
}

func validateConfig(options config.Consolidator) error {
	if options.Address == "" {
		return errors.New("consolidator.address must not be empty")
	}
	if len(options.Brokers) == 0 {
		return errors.New("consolidator.brokers must contain at least one address")
	}
	for _, broker := range options.Brokers {
		if strings.TrimSpace(broker) == "" {
			return errors.New("consolidator.brokers must not contain an empty address")
		}
	}
	if options.EventsTopic == "" {
		return errors.New("consolidator.events_topic must not be empty")
	}
	if options.SnapshotsTopic == "" {
		return errors.New("consolidator.snapshots_topic must not be empty")
	}
	if options.StartupTimeout.Duration <= 0 {
		return errors.New("consolidator.startup_timeout must be greater than zero")
	}
	return nil
}
