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

// Package state defines the Kafka example's application-owned state wire
// format. It is internal because neither the framework nor users of the example
// should depend on it as a public protocol.
package state

import (
	"context"
	"encoding/json"
	"fmt"

	websubhub "github.com/ayeshLK/lib-websubhub"
)

// SubscriptionStatus is application-owned delivery state.
type SubscriptionStatus string

// SubscriptionStatusStale marks a subscription whose delivery worker stopped
// after exhausting its bounded retries.
const SubscriptionStatusStale SubscriptionStatus = "stale"

// Subscription extends a verified WebSub subscription with example-specific
// ownership and delivery state. The embedded fields remain flat in JSON.
type Subscription struct {
	websubhub.VerifiedSubscription
	ServerID string             `json:"server_id"`
	Status   SubscriptionStatus `json:"status,omitempty"`
}

// NewSubscription returns the flat state event for a server-owned subscription.
func NewSubscription(verified websubhub.VerifiedSubscription, serverID string) Subscription {
	return Subscription{
		VerifiedSubscription: verified,
		ServerID:             serverID,
	}
}

// NewStaleSubscription returns the flat state event used to mark a verified
// subscription stale.
func NewStaleSubscription(verified websubhub.VerifiedSubscription, serverID string) Subscription {
	return Subscription{
		VerifiedSubscription: verified,
		ServerID:             serverID,
		Status:               SubscriptionStatusStale,
	}
}

// Snapshot is the consolidator's immutable view of application state.
type Snapshot struct {
	Revision      uint64                        `json:"revision"`
	Topics        []websubhub.TopicRegistration `json:"topics"`
	Subscriptions []Subscription                `json:"subscriptions"`
}

// Clone returns a snapshot whose slices do not alias the receiver.
func (s Snapshot) Clone() Snapshot {
	return Snapshot{
		Revision:      s.Revision,
		Topics:        append([]websubhub.TopicRegistration(nil), s.Topics...),
		Subscriptions: append([]Subscription(nil), s.Subscriptions...),
	}
}

// Consumer applies one decoded state event.
type Consumer interface {
	ApplyTopicRegistration(context.Context, websubhub.TopicRegistration) error
	ApplyTopicDeregistration(context.Context, websubhub.TopicDeregistration) error
	ApplySubscription(context.Context, Subscription) error
	ApplyStaleSubscription(context.Context, Subscription) error
	ApplyUnsubscription(context.Context, websubhub.VerifiedUnsubscription) error
}

// ApplyEvent decodes a concrete state-event record and dispatches it to a
// consumer without introducing an envelope into the Kafka record format.
func ApplyEvent(ctx context.Context, payload []byte, consumer Consumer) error {
	var discriminator struct {
		Mode   websubhub.Mode
		Status SubscriptionStatus `json:"status"`
	}
	if err := json.Unmarshal(payload, &discriminator); err != nil {
		return fmt.Errorf("decode event discriminator: %w", err)
	}

	if discriminator.Status != "" {
		if discriminator.Mode != websubhub.ModeSubscribe || discriminator.Status != SubscriptionStatusStale {
			return fmt.Errorf("unsupported subscription status %q", discriminator.Status)
		}
		var subscription Subscription
		if err := json.Unmarshal(payload, &subscription); err != nil {
			return fmt.Errorf("decode stale subscription: %w", err)
		}
		return consumer.ApplyStaleSubscription(ctx, subscription)
	}

	switch discriminator.Mode {
	case websubhub.ModeRegister:
		var registration websubhub.TopicRegistration
		if err := json.Unmarshal(payload, &registration); err != nil {
			return fmt.Errorf("decode topic registration: %w", err)
		}
		return consumer.ApplyTopicRegistration(ctx, registration)
	case websubhub.ModeDeregister:
		var deregistration websubhub.TopicDeregistration
		if err := json.Unmarshal(payload, &deregistration); err != nil {
			return fmt.Errorf("decode topic deregistration: %w", err)
		}
		return consumer.ApplyTopicDeregistration(ctx, deregistration)
	case websubhub.ModeSubscribe:
		var subscription Subscription
		if err := json.Unmarshal(payload, &subscription); err != nil {
			return fmt.Errorf("decode verified subscription: %w", err)
		}
		return consumer.ApplySubscription(ctx, subscription)
	case websubhub.ModeUnsubscribe:
		var unsubscription websubhub.VerifiedUnsubscription
		if err := json.Unmarshal(payload, &unsubscription); err != nil {
			return fmt.Errorf("decode verified unsubscription: %w", err)
		}
		return consumer.ApplyUnsubscription(ctx, unsubscription)
	default:
		return fmt.Errorf("unsupported event mode %q", discriminator.Mode)
	}
}
