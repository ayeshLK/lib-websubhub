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

// Package config loads the Kafka example's runtime configuration.
package config

import (
	"fmt"
	"time"

	"github.com/BurntSushi/toml"
)

const fileName = "Config.toml"

// WebSubHub configures the WebSub hub process.
type WebSubHub struct {
	ServerID         string   `toml:"server_id"`
	Address          string   `toml:"address"`
	Path             string   `toml:"path"`
	HubURL           string   `toml:"hub_url"`
	Brokers          []string `toml:"brokers"`
	EventsTopic      string   `toml:"events_topic"`
	ConsolidatorURL  string   `toml:"consolidator_url"`
	DeliveryAttempts int      `toml:"delivery_attempts"`
	RetryBackoff     Duration `toml:"retry_backoff"`
	StartupTimeout   Duration `toml:"startup_timeout"`
}

// Consolidator configures the state consolidator process.
type Consolidator struct {
	Address        string   `toml:"address"`
	Brokers        []string `toml:"brokers"`
	EventsTopic    string   `toml:"events_topic"`
	SnapshotsTopic string   `toml:"snapshots_topic"`
	StartupTimeout Duration `toml:"startup_timeout"`
}

// Duration is a TOML string parsed with time.ParseDuration.
type Duration struct {
	time.Duration
}

// UnmarshalText parses a Go duration such as "500ms" or "30s".
func (duration *Duration) UnmarshalText(text []byte) error {
	value, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	duration.Duration = value
	return nil
}

// LoadWebSubHub reads the websubhub table from Config.toml in the working directory.
func LoadWebSubHub() (WebSubHub, error) {
	var document struct {
		WebSubHub WebSubHub `toml:"websubhub"`
	}
	if err := decode(&document); err != nil {
		return WebSubHub{}, err
	}
	return document.WebSubHub, nil
}

// LoadConsolidator reads the consolidator table from Config.toml in the working directory.
func LoadConsolidator() (Consolidator, error) {
	var document struct {
		Consolidator Consolidator `toml:"consolidator"`
	}
	if err := decode(&document); err != nil {
		return Consolidator{}, err
	}
	return document.Consolidator, nil
}

func decode(destination any) error {
	metadata, err := toml.DecodeFile(fileName, destination)
	if err != nil {
		return fmt.Errorf("load %s: %w", fileName, err)
	}
	if undecoded := metadata.Undecoded(); len(undecoded) != 0 {
		return fmt.Errorf("load %s: unknown configuration key %q", fileName, undecoded[0])
	}
	return nil
}
