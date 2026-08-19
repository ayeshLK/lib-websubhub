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
	"encoding/json"
	"errors"
	"fmt"

	websubhub "github.com/ayeshLK/lib-websubhub"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	eventsPartition        int32 = 0
	kafkaContentTypeHeader       = "Content-Type"
	maxContentBatchRecords       = 5
)

type subscriptionStatus string

const subscriptionStatusStale subscriptionStatus = "stale"

// subscriptionState extends the protocol subscription with application-owned
// delivery state. Its embedded fields remain flat in the Kafka JSON record.
type subscriptionState struct {
	websubhub.VerifiedSubscription
	Status subscriptionStatus `json:"status"`
}

func newStaleSubscription(verified websubhub.VerifiedSubscription) subscriptionState {
	return subscriptionState{
		VerifiedSubscription: verified,
		Status:               subscriptionStatusStale,
	}
}

type contentMessage struct {
	ContentType string
	Body        []byte
}

type contentBatchConsumer func(context.Context, []contentMessage) error

type eventConsumer interface {
	applyTopicRegistration(context.Context, websubhub.TopicRegistration) error
	applyTopicDeregistration(context.Context, websubhub.TopicDeregistration) error
	applySubscription(context.Context, websubhub.VerifiedSubscription) error
	applyStaleSubscription(context.Context, subscriptionState) error
	applyUnsubscription(context.Context, websubhub.VerifiedUnsubscription) error
}

type messageBroker interface {
	PublishTopicRegistration(context.Context, websubhub.TopicRegistration) error
	PublishTopicDeregistration(context.Context, websubhub.TopicDeregistration) error
	PublishSubscription(context.Context, websubhub.VerifiedSubscription) error
	PublishStaleSubscription(context.Context, subscriptionState) error
	PublishUnsubscription(context.Context, websubhub.VerifiedUnsubscription) error
	PublishContent(context.Context, string, contentMessage) error
	ReplayEvents(context.Context, eventConsumer) error
	ConsumeEvents(context.Context, eventConsumer) error
	ConsumeSubscription(context.Context, string, string, contentBatchConsumer) error
	Close()
}

type kafkaBrokerOptions struct {
	brokers     []string
	eventsTopic string
}

type kafkaBroker struct {
	brokers      []string
	producer     *kgo.Client
	eventsClient *kgo.Client
	eventsTopic  string
	eventsOffset int64
}

func newKafkaBroker(ctx context.Context, options kafkaBrokerOptions) (*kafkaBroker, error) {
	if len(options.brokers) == 0 {
		return nil, errors.New("at least one Kafka broker is required")
	}
	if options.eventsTopic == "" {
		return nil, errors.New("Kafka event topic must not be empty")
	}

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(options.brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}
	if err := producer.Ping(ctx); err != nil {
		producer.Close()
		return nil, fmt.Errorf("connect to Kafka: %w", err)
	}

	eventsClient, err := kgo.NewClient(
		kgo.SeedBrokers(options.brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			options.eventsTopic: {eventsPartition: kgo.NewOffset().AtStart()},
		}),
	)
	if err != nil {
		producer.Close()
		return nil, fmt.Errorf("create Kafka event consumer: %w", err)
	}

	return &kafkaBroker{
		brokers:      append([]string(nil), options.brokers...),
		producer:     producer,
		eventsClient: eventsClient,
		eventsTopic:  options.eventsTopic,
	}, nil
}

func (b *kafkaBroker) PublishTopicRegistration(ctx context.Context, registration websubhub.TopicRegistration) error {
	return b.produceJSON(ctx, b.eventsTopic, registration)
}

func (b *kafkaBroker) PublishTopicDeregistration(ctx context.Context, deregistration websubhub.TopicDeregistration) error {
	return b.produceJSON(ctx, b.eventsTopic, deregistration)
}

func (b *kafkaBroker) PublishSubscription(ctx context.Context, subscription websubhub.VerifiedSubscription) error {
	return b.produceJSON(ctx, b.eventsTopic, subscription)
}

func (b *kafkaBroker) PublishStaleSubscription(ctx context.Context, subscription subscriptionState) error {
	return b.produceJSON(ctx, b.eventsTopic, subscription)
}

func (b *kafkaBroker) PublishUnsubscription(ctx context.Context, unsubscription websubhub.VerifiedUnsubscription) error {
	return b.produceJSON(ctx, b.eventsTopic, unsubscription)
}

func (b *kafkaBroker) PublishContent(ctx context.Context, topic string, content contentMessage) error {
	record := &kgo.Record{
		Topic:     topic,
		Partition: eventsPartition,
		Value:     append([]byte(nil), content.Body...),
		Headers: []kgo.RecordHeader{{
			Key:   kafkaContentTypeHeader,
			Value: []byte(content.ContentType),
		}},
	}
	if err := b.producer.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce Kafka content to %s: %w", topic, err)
	}
	return nil
}

func (b *kafkaBroker) produceJSON(ctx context.Context, topic string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode Kafka record for %s: %w", topic, err)
	}
	record := &kgo.Record{Topic: topic, Partition: eventsPartition, Value: payload}
	if err := b.producer.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce Kafka record to %s: %w", topic, err)
	}
	return nil
}

func (b *kafkaBroker) ReplayEvents(ctx context.Context, apply eventConsumer) error {
	for {
		fetches := b.eventsClient.PollFetches(ctx)
		if err := firstFetchError(ctx, fetches); err != nil {
			return fmt.Errorf("replay Kafka events: %w", err)
		}

		caughtUp := false
		var applyErr error
		fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
			if applyErr != nil || partition.Topic != b.eventsTopic || partition.Partition != eventsPartition {
				return
			}
			if b.eventsOffset < partition.LogStartOffset {
				b.eventsOffset = partition.LogStartOffset
			}
			for _, record := range partition.Records {
				if err := applyEvent(ctx, record.Value, apply); err != nil {
					applyErr = fmt.Errorf("apply event at offset %d: %w", record.Offset, err)
					return
				}
				b.eventsOffset = record.Offset + 1
			}
			caughtUp = b.eventsOffset >= partition.HighWatermark
		})
		if applyErr != nil {
			return applyErr
		}
		if caughtUp {
			return nil
		}
	}
}

func (b *kafkaBroker) ConsumeEvents(ctx context.Context, apply eventConsumer) error {
	for {
		fetches := b.eventsClient.PollFetches(ctx)
		if err := firstFetchError(ctx, fetches); err != nil {
			return fmt.Errorf("consume Kafka events: %w", err)
		}
		var applyErr error
		fetches.EachRecord(func(record *kgo.Record) {
			if applyErr != nil || record.Topic != b.eventsTopic || record.Partition != eventsPartition || record.Offset < b.eventsOffset {
				return
			}
			if err := applyEvent(ctx, record.Value, apply); err != nil {
				applyErr = fmt.Errorf("apply event at offset %d: %w", record.Offset, err)
				return
			}
			b.eventsOffset = record.Offset + 1
		})
		if applyErr != nil {
			return applyErr
		}
	}
}

func (b *kafkaBroker) ConsumeSubscription(ctx context.Context, topic, group string, consume contentBatchConsumer) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(b.brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtEnd()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return fmt.Errorf("create Kafka subscription consumer: %w", err)
	}
	defer client.Close()

	for {
		fetches := client.PollRecords(ctx, maxContentBatchRecords)
		if err := firstFetchError(ctx, fetches); err != nil {
			return fmt.Errorf("poll Kafka content: %w", err)
		}

		var records []*kgo.Record
		var messages []contentMessage
		fetches.EachRecord(func(record *kgo.Record) {
			records = append(records, record)
			messages = append(messages, contentMessage{
				ContentType: recordHeader(record, kafkaContentTypeHeader),
				Body:        append([]byte(nil), record.Value...),
			})
		})
		if len(records) == 0 {
			client.AllowRebalance()
			continue
		}
		if err := consume(ctx, messages); err != nil {
			client.AllowRebalance()
			return fmt.Errorf("deliver Kafka content batch: %w", err)
		}
		if err := client.CommitRecords(ctx, records...); err != nil {
			client.AllowRebalance()
			return fmt.Errorf("commit Kafka content batch: %w", err)
		}
		client.AllowRebalance()
	}
}

func recordHeader(record *kgo.Record, name string) string {
	for _, header := range record.Headers {
		if header.Key == name {
			return string(header.Value)
		}
	}
	return ""
}

func firstFetchError(ctx context.Context, fetches kgo.Fetches) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if fetchErrors := fetches.Errors(); len(fetchErrors) > 0 {
		return fetchErrors[0].Err
	}
	return nil
}

func applyEvent(ctx context.Context, payload []byte, consumer eventConsumer) error {
	var discriminator struct {
		Mode   websubhub.Mode
		Status subscriptionStatus `json:"status"`
	}
	if err := json.Unmarshal(payload, &discriminator); err != nil {
		return fmt.Errorf("decode event discriminator: %w", err)
	}

	if discriminator.Status != "" {
		if discriminator.Mode != websubhub.ModeSubscribe || discriminator.Status != subscriptionStatusStale {
			return fmt.Errorf("unsupported subscription status %q", discriminator.Status)
		}
		var subscription subscriptionState
		if err := json.Unmarshal(payload, &subscription); err != nil {
			return fmt.Errorf("decode stale subscription: %w", err)
		}
		return consumer.applyStaleSubscription(ctx, subscription)
	}

	switch discriminator.Mode {
	case websubhub.ModeRegister:
		var registration websubhub.TopicRegistration
		if err := json.Unmarshal(payload, &registration); err != nil {
			return fmt.Errorf("decode topic registration: %w", err)
		}
		return consumer.applyTopicRegistration(ctx, registration)
	case websubhub.ModeDeregister:
		var deregistration websubhub.TopicDeregistration
		if err := json.Unmarshal(payload, &deregistration); err != nil {
			return fmt.Errorf("decode topic deregistration: %w", err)
		}
		return consumer.applyTopicDeregistration(ctx, deregistration)
	case websubhub.ModeSubscribe:
		var subscription websubhub.VerifiedSubscription
		if err := json.Unmarshal(payload, &subscription); err != nil {
			return fmt.Errorf("decode verified subscription: %w", err)
		}
		return consumer.applySubscription(ctx, subscription)
	case websubhub.ModeUnsubscribe:
		var unsubscription websubhub.VerifiedUnsubscription
		if err := json.Unmarshal(payload, &unsubscription); err != nil {
			return fmt.Errorf("decode verified unsubscription: %w", err)
		}
		return consumer.applyUnsubscription(ctx, unsubscription)
	default:
		return fmt.Errorf("unsupported event mode %q", discriminator.Mode)
	}
}

func (b *kafkaBroker) Close() {
	b.eventsClient.Close()
	b.producer.Close()
}
