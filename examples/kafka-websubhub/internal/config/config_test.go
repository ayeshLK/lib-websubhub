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

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadWebSubHub(t *testing.T) {
	directory := t.TempDir()
	writeConfig(t, directory, `[websubhub]
server_id = "server-1"
address = ":9090"
path = "/hub"
hub_url = "http://localhost:9090/hub"
brokers = ["broker-1:9092", "broker-2:9092"]
events_topic = "events"
consolidator_url = "http://localhost:9091/state-snapshot"
delivery_attempts = 4
retry_backoff = "250ms"
startup_timeout = "20s"
`)
	withWorkingDirectory(t, directory)

	configuration, err := LoadWebSubHub()
	if err != nil {
		t.Fatalf("LoadWebSubHub() error = %v", err)
	}
	if configuration.ServerID != "server-1" {
		t.Fatalf("ServerID = %q, want server-1", configuration.ServerID)
	}
	if configuration.RetryBackoff.Duration != 250*time.Millisecond {
		t.Fatalf("RetryBackoff = %v, want 250ms", configuration.RetryBackoff.Duration)
	}
	if len(configuration.Brokers) != 2 {
		t.Fatalf("Brokers = %#v, want two brokers", configuration.Brokers)
	}
}

func TestLoadConsolidator(t *testing.T) {
	directory := t.TempDir()
	writeConfig(t, directory, `[consolidator]
address = ":9091"
brokers = ["broker-1:9092"]
events_topic = "events"
snapshots_topic = "snapshots"
startup_timeout = "30s"
`)
	withWorkingDirectory(t, directory)

	configuration, err := LoadConsolidator()
	if err != nil {
		t.Fatalf("LoadConsolidator() error = %v", err)
	}
	if configuration.SnapshotsTopic != "snapshots" {
		t.Fatalf("SnapshotsTopic = %q, want snapshots", configuration.SnapshotsTopic)
	}
}

func TestLoadRejectsUnknownKey(t *testing.T) {
	directory := t.TempDir()
	writeConfig(t, directory, "[websubhub]\nunknown = true\n")
	withWorkingDirectory(t, directory)

	_, err := LoadWebSubHub()
	if err == nil || !strings.Contains(err.Error(), "unknown configuration key") {
		t.Fatalf("LoadWebSubHub() error = %v, want unknown-key error", err)
	}
}

func writeConfig(t *testing.T, directory string, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(directory, fileName), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func withWorkingDirectory(t *testing.T, directory string) {
	t.Helper()
	original, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(directory); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(original); err != nil {
			t.Errorf("restore working directory: %v", err)
		}
	})
}
