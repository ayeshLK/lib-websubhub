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

package consolidator

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"

	websubhub "github.com/ayeshLK/lib-websubhub"
	"github.com/ayeshLK/lib-websubhub/examples/kafka-websubhub/internal/state"
)

func TestRetainLatestSnapshot(t *testing.T) {
	t.Parallel()
	snapshots := []state.Snapshot{
		{Revision: 4},
		{Revision: 2},
		{Revision: 7},
	}
	var latest state.Snapshot
	found := false
	for _, snapshot := range snapshots {
		payload, err := json.Marshal(snapshot)
		if err != nil {
			t.Fatalf("encode snapshot: %v", err)
		}
		latest, found, err = retainLatestSnapshot(latest, found, payload)
		if err != nil {
			t.Fatalf("retain snapshot: %v", err)
		}
	}
	if !found || latest.Revision != 7 {
		t.Fatalf("latest snapshot = %+v, found = %t", latest, found)
	}
}

func TestRetainLatestSnapshotRejectsMalformedJSON(t *testing.T) {
	t.Parallel()
	original := state.Snapshot{Revision: 3}
	latest, found, err := retainLatestSnapshot(original, true, []byte("not-json"))
	if err == nil {
		t.Fatal("malformed snapshot unexpectedly accepted")
	}
	if !found || latest.Revision != original.Revision {
		t.Fatalf("latest snapshot = %+v, found = %t", latest, found)
	}
}

type recordingBroker struct {
	snapshot state.Snapshot
}

func (*recordingBroker) LoadSnapshot(context.Context) (state.Snapshot, error) {
	return state.Snapshot{}, nil
}

func (b *recordingBroker) PublishSnapshot(_ context.Context, snapshot state.Snapshot) error {
	b.snapshot = snapshot.Clone()
	return nil
}

func (*recordingBroker) ReplayEvents(context.Context, EventHandler) error  { return nil }
func (*recordingBroker) ConsumeEvents(context.Context, EventHandler) error { return nil }
func (*recordingBroker) Close()                                            {}

func TestConsumeEventIncrementsSnapshotRevision(t *testing.T) {
	broker := &recordingBroker{}
	consolidator := &Consolidator{
		topics:        make(map[string]websubhub.TopicRegistration),
		subscriptions: make(map[string]state.Subscription),
		revision:      6,
		broker:        broker,
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	payload, err := json.Marshal(websubhub.TopicRegistration{
		Mode:  websubhub.ModeRegister,
		Topic: "https://publisher.example/topics/revision",
	})
	if err != nil {
		t.Fatalf("encode state event: %v", err)
	}
	if err := consolidator.consumeEvent(context.Background(), 12, payload); err != nil {
		t.Fatalf("consume state event: %v", err)
	}
	if broker.snapshot.Revision != 7 {
		t.Fatalf("published revision = %d, want 7", broker.snapshot.Revision)
	}
	if len(broker.snapshot.Topics) != 1 {
		t.Fatalf("published topics = %d, want 1", len(broker.snapshot.Topics))
	}
}

func TestConsumeOwnedSubscriptionPersistsServerID(t *testing.T) {
	broker := &recordingBroker{}
	consolidator := &Consolidator{
		topics:        make(map[string]websubhub.TopicRegistration),
		subscriptions: make(map[string]state.Subscription),
		broker:        broker,
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	verified := websubhub.VerifiedSubscription{
		Subscription: websubhub.Subscription{
			Hub:      "https://hub.example/hub",
			Mode:     websubhub.ModeSubscribe,
			Topic:    "https://publisher.example/topics/owned",
			Callback: "https://subscriber.example/callback",
		},
		EffectiveLeaseSeconds: "300",
		LeaseStartedAt:        time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC),
	}
	payload, err := json.Marshal(state.NewSubscription(verified, "server-2"))
	if err != nil {
		t.Fatalf("encode subscription event: %v", err)
	}
	if err := consolidator.consumeEvent(context.Background(), 13, payload); err != nil {
		t.Fatalf("consume subscription event: %v", err)
	}
	if len(broker.snapshot.Subscriptions) != 1 || broker.snapshot.Subscriptions[0].ServerID != "server-2" {
		t.Fatalf("published subscriptions = %+v", broker.snapshot.Subscriptions)
	}
}
