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

package websubhub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/ayeshLK/lib-websubhub/examples/kafka-websubhub/internal/state"
)

const maxSnapshotBody = 16 << 20 // 16 MiB

// SnapshotSource supplies the latest consolidated application state.
type SnapshotSource interface {
	Snapshot(context.Context) (state.Snapshot, error)
}

// HTTPSnapshotSource retrieves a state snapshot from the consolidator over
// HTTP.
type HTTPSnapshotSource struct {
	endpoint string
	client   *http.Client
}

// NewHTTPSnapshotSource validates an absolute HTTP(S) endpoint and returns a
// bounded, redirect-refusing snapshot client.
func NewHTTPSnapshotSource(endpoint string, client *http.Client) (*HTTPSnapshotSource, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, errors.New("snapshot endpoint must be an absolute HTTP(S) URL")
	}
	if client == nil {
		client = &http.Client{}
	} else {
		clone := *client
		client = &clone
	}
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &HTTPSnapshotSource{endpoint: endpoint, client: client}, nil
}

// Snapshot retrieves and validates the consolidator's current state.
func (s *HTTPSnapshotSource) Snapshot(ctx context.Context) (state.Snapshot, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, s.endpoint, nil)
	if err != nil {
		return state.Snapshot{}, fmt.Errorf("create snapshot request: %w", err)
	}
	response, err := s.client.Do(request)
	if err != nil {
		return state.Snapshot{}, fmt.Errorf("retrieve consolidator snapshot: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return state.Snapshot{}, fmt.Errorf("consolidator returned HTTP %d", response.StatusCode)
	}

	limited := io.LimitReader(response.Body, maxSnapshotBody+1)
	payload, err := io.ReadAll(limited)
	if err != nil {
		return state.Snapshot{}, fmt.Errorf("read consolidator snapshot: %w", err)
	}
	if len(payload) > maxSnapshotBody {
		return state.Snapshot{}, fmt.Errorf("consolidator snapshot exceeds %d bytes", maxSnapshotBody)
	}
	var snapshot state.Snapshot
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return state.Snapshot{}, fmt.Errorf("decode consolidator snapshot: %w", err)
	}
	return snapshot, nil
}
