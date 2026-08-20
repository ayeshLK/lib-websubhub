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
	"sync"

	websubhub "github.com/ayeshLK/lib-websubhub"
	"github.com/ayeshLK/lib-websubhub/examples/kafka-websubhub/internal/state"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	eventsPartition        int32 = 0
	kafkaContentTypeHeader       = "Content-Type"
	maxContentBatchRecords       = 5
)

// ContentMessage is the Kafka representation delivered to a subscriber.
type ContentMessage struct {
	ContentType string
	Body        []byte
}

// ContentBatchConsumer delivers one bounded Kafka batch.
type ContentBatchConsumer func(context.Context, []ContentMessage) error

// Broker is the hub process's application-owned Kafka boundary.
type Broker interface {
	PublishTopicRegistration(context.Context, websubhub.TopicRegistration) error
	PublishTopicDeregistration(context.Context, websubhub.TopicDeregistration) error
	PublishSubscription(context.Context, state.Subscription) error
	PublishStaleSubscription(context.Context, state.Subscription) error
	PublishUnsubscription(context.Context, websubhub.VerifiedUnsubscription) error
	PublishContent(context.Context, string, ContentMessage) error
	ReplayEvents(context.Context, state.Consumer) error
	ConsumeEvents(context.Context, state.Consumer) error
	ConsumeSubscription(context.Context, string, string, ContentBatchConsumer) error
	Close()
}

// KafkaBrokerOptions configures the hub process's Kafka adapter.
type KafkaBrokerOptions struct {
	Brokers     []string
	EventsTopic string
}

// KafkaBroker publishes state and content and consumes live state changes and
// per-subscription content. Snapshot construction belongs to the consolidator.
type KafkaBroker struct {
	brokers      []string
	producer     *kgo.Client
	eventsTopic  string
	mu           sync.Mutex
	eventsOffset int64
	eventsClient *kgo.Client
	closeOnce    sync.Once
}

// NewKafkaBroker connects the hub process to Kafka.
func NewKafkaBroker(ctx context.Context, options KafkaBrokerOptions) (*KafkaBroker, error) {
	if len(options.Brokers) == 0 {
		return nil, errors.New("at least one Kafka broker is required")
	}
	if options.EventsTopic == "" {
		return nil, errors.New("Kafka event topic must not be empty")
	}

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(options.Brokers...),
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

	return &KafkaBroker{
		brokers:     append([]string(nil), options.Brokers...),
		producer:    producer,
		eventsTopic: options.EventsTopic,
	}, nil
}

func (b *KafkaBroker) PublishTopicRegistration(ctx context.Context, registration websubhub.TopicRegistration) error {
	return b.produceJSON(ctx, b.eventsTopic, registration)
}

func (b *KafkaBroker) PublishTopicDeregistration(ctx context.Context, deregistration websubhub.TopicDeregistration) error {
	return b.produceJSON(ctx, b.eventsTopic, deregistration)
}

func (b *KafkaBroker) PublishSubscription(ctx context.Context, subscription state.Subscription) error {
	return b.produceJSON(ctx, b.eventsTopic, subscription)
}

func (b *KafkaBroker) PublishStaleSubscription(ctx context.Context, subscription state.Subscription) error {
	return b.produceJSON(ctx, b.eventsTopic, subscription)
}

func (b *KafkaBroker) PublishUnsubscription(ctx context.Context, unsubscription websubhub.VerifiedUnsubscription) error {
	return b.produceJSON(ctx, b.eventsTopic, unsubscription)
}

func (b *KafkaBroker) PublishContent(ctx context.Context, topic string, content ContentMessage) error {
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

func (b *KafkaBroker) produceJSON(ctx context.Context, topic string, value any) error {
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

// ReplayEvents applies retained state events through a captured log boundary.
func (b *KafkaBroker) ReplayEvents(ctx context.Context, apply state.Consumer) error {
	replayEnd, err := b.eventLogEnd(ctx)
	if err != nil {
		return fmt.Errorf("resolve Kafka event replay boundary: %w", err)
	}
	if b.eventsOffset >= replayEnd {
		return nil
	}
	return b.consumeEvents(ctx, replayEnd, apply)
}

// ConsumeEvents tails state events after startup replay.
func (b *KafkaBroker) ConsumeEvents(ctx context.Context, apply state.Consumer) error {
	return b.consumeEvents(ctx, -1, apply)
}

func (b *KafkaBroker) consumeEvents(ctx context.Context, replayEnd int64, apply state.Consumer) error {
	offset := kgo.NewOffset().At(b.eventsOffset)
	if b.eventsOffset == 0 {
		offset = kgo.NewOffset().AtStart()
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(b.brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			b.eventsTopic: {eventsPartition: offset},
		}),
	)
	if err != nil {
		return fmt.Errorf("create Kafka event consumer: %w", err)
	}
	b.mu.Lock()
	b.eventsClient = client
	b.mu.Unlock()
	defer func() {
		client.Close()
		b.mu.Lock()
		if b.eventsClient == client {
			b.eventsClient = nil
		}
		b.mu.Unlock()
	}()

	for {
		fetches := client.PollFetches(ctx)
		if err := firstFetchError(ctx, fetches); err != nil {
			return fmt.Errorf("consume Kafka events: %w", err)
		}
		var applyErr error
		fetches.EachRecord(func(record *kgo.Record) {
			if applyErr != nil || record.Topic != b.eventsTopic || record.Partition != eventsPartition || record.Offset < b.eventsOffset || (replayEnd >= 0 && record.Offset >= replayEnd) {
				return
			}
			if err := state.ApplyEvent(ctx, record.Value, apply); err != nil {
				applyErr = fmt.Errorf("apply event at offset %d: %w", record.Offset, err)
				return
			}
			b.eventsOffset = record.Offset + 1
		})
		if applyErr != nil {
			return applyErr
		}
		if replayEnd >= 0 && b.eventsOffset >= replayEnd {
			return nil
		}
	}
}

func (b *KafkaBroker) eventLogEnd(ctx context.Context) (int64, error) {
	request := kmsg.NewPtrListOffsetsRequest()
	topic := kmsg.NewListOffsetsRequestTopic()
	topic.Topic = b.eventsTopic
	partition := kmsg.NewListOffsetsRequestTopicPartition()
	partition.Partition = eventsPartition
	partition.Timestamp = -1
	topic.Partitions = append(topic.Partitions, partition)
	request.Topics = append(request.Topics, topic)
	rawResponse, err := b.producer.Request(ctx, request)
	if err != nil {
		return 0, err
	}
	response, ok := rawResponse.(*kmsg.ListOffsetsResponse)
	if !ok {
		return 0, fmt.Errorf("unexpected Kafka response %T", rawResponse)
	}
	for _, responseTopic := range response.Topics {
		if responseTopic.Topic != b.eventsTopic {
			continue
		}
		for _, responsePartition := range responseTopic.Partitions {
			if responsePartition.Partition != eventsPartition {
				continue
			}
			if err := kerr.ErrorForCode(responsePartition.ErrorCode); err != nil {
				return 0, err
			}
			if responsePartition.Offset < 0 {
				return 0, fmt.Errorf("invalid Kafka log-end offset %d", responsePartition.Offset)
			}
			return responsePartition.Offset, nil
		}
	}
	return 0, errors.New("Kafka event partition missing from list-offsets response")
}

func (b *KafkaBroker) ConsumeSubscription(ctx context.Context, topic, group string, consume ContentBatchConsumer) error {
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
		var messages []ContentMessage
		fetches.EachRecord(func(record *kgo.Record) {
			records = append(records, record)
			messages = append(messages, ContentMessage{
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

// Close releases the hub process's Kafka clients.
func (b *KafkaBroker) Close() {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		if b.eventsClient != nil {
			b.eventsClient.Close()
		}
		b.mu.Unlock()
		b.producer.Close()
	})
}
