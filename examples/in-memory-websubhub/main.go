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
	address         string
	path            string
	hubURL          string
	deliveryWorkers int
}

func main() {
	if err := run(); err != nil {
		slog.Error("sample hub stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	options := commandOptions{}
	flag.StringVar(&options.address, "addr", ":9090", "HTTP listen address")
	flag.StringVar(&options.path, "path", "/hub", "hub HTTP path")
	flag.StringVar(&options.hubURL, "hub-url", "http://localhost:9090/hub", "public absolute hub URL used in verification and delivery")
	flag.IntVar(&options.deliveryWorkers, "delivery-workers", defaultWorkers, "number of concurrent delivery workers")
	flag.Parse()
	if err := validateCommandOptions(options); err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	application, err := newMemoryHub(hubOptions{
		hubURL:          options.hubURL,
		deliveryWorkers: options.deliveryWorkers,
		logger:          logger,
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
		logger.Info("sample WebSub hub listening", "address", options.address, "path", options.path, "hub_url", options.hubURL)
		serverResult <- server.ListenAndServe()
	}()

	var serveErr error
	select {
	case <-rootContext.Done():
	case serveErr = <-serverResult:
	}

	shutdownContext, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	shutdownErr := server.Shutdown(shutdownContext)
	handlerErr := handler.Close(shutdownContext)
	applicationErr := application.Close(shutdownContext)
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return serveErr
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
		return fmt.Errorf("-hub-url must be an absolute HTTP(S) URL")
	}
	if parsed.Path != options.path {
		return fmt.Errorf("-hub-url path %q must match -path %q", parsed.Path, options.path)
	}
	if options.deliveryWorkers <= 0 {
		return errors.New("-delivery-workers must be greater than zero")
	}
	return nil
}
