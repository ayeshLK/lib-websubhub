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
	"errors"
	"fmt"
	"sync"

	"github.com/ayeshLK/lib-websubhub/examples/kafka-websubhub/internal/state"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	statePartition int32 = 0
)

// KafkaBrokerOptions configures the consolidator's Kafka adapter.
type KafkaBrokerOptions struct {
	Brokers        []string
	EventsTopic    string
	SnapshotsTopic string
}

// KafkaBroker consumes state events and persists revisioned system snapshots.
type KafkaBroker struct {
	brokers        []string
	eventsTopic    string
	snapshotsTopic string
	producer       *kgo.Client

	mu           sync.Mutex
	eventsOffset int64
	consumer     *kgo.Client
	closeOnce    sync.Once
}

// NewKafkaBroker connects the consolidator to Kafka.
func NewKafkaBroker(ctx context.Context, options KafkaBrokerOptions) (*KafkaBroker, error) {
	if len(options.Brokers) == 0 {
		return nil, errors.New("at least one Kafka broker is required")
	}
	if options.EventsTopic == "" {
		return nil, errors.New("Kafka event topic must not be empty")
	}
	if options.SnapshotsTopic == "" {
		return nil, errors.New("Kafka snapshot topic must not be empty")
	}
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(options.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka consolidator client: %w", err)
	}
	if err := producer.Ping(ctx); err != nil {
		producer.Close()
		return nil, fmt.Errorf("connect consolidator to Kafka: %w", err)
	}
	return &KafkaBroker{
		brokers:        append([]string(nil), options.Brokers...),
		eventsTopic:    options.EventsTopic,
		snapshotsTopic: options.SnapshotsTopic,
		producer:       producer,
	}, nil
}

// LoadSnapshot polls every snapshot-topic partition from its beginning and
// retains the complete snapshot with the greatest revision.
func (b *KafkaBroker) LoadSnapshot(ctx context.Context) (state.Snapshot, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(b.brokers...),
		kgo.ConsumeTopics(b.snapshotsTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return state.Snapshot{}, fmt.Errorf("create Kafka snapshot reader: %w", err)
	}
	defer client.Close()

	var latest state.Snapshot
	found := false
	for {
		fetches := client.PollFetches(ctx)
		if err := fetchError(ctx, fetches); err != nil {
			return state.Snapshot{}, fmt.Errorf("read Kafka snapshots: %w", err)
		}
		if fetches.NumRecords() == 0 {
			return latest, nil
		}

		var decodeErr error
		fetches.EachRecord(func(record *kgo.Record) {
			if decodeErr != nil || record.Topic != b.snapshotsTopic {
				return
			}
			candidate, candidateFound, err := retainLatestSnapshot(latest, found, record.Value)
			if err != nil {
				decodeErr = fmt.Errorf("decode Kafka snapshot at partition %d offset %d: %w", record.Partition, record.Offset, err)
				return
			}
			latest, found = candidate, candidateFound
		})
		if decodeErr != nil {
			return state.Snapshot{}, decodeErr
		}
	}
}

func retainLatestSnapshot(latest state.Snapshot, found bool, payload []byte) (state.Snapshot, bool, error) {
	var candidate state.Snapshot
	if err := json.Unmarshal(payload, &candidate); err != nil {
		return latest, found, err
	}
	if !found || candidate.Revision > latest.Revision {
		return candidate, true, nil
	}
	return latest, found, nil
}

// PublishSnapshot persists one complete revisioned state snapshot.
func (b *KafkaBroker) PublishSnapshot(ctx context.Context, snapshot state.Snapshot) error {
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode Kafka state snapshot: %w", err)
	}
	record := &kgo.Record{
		Topic: b.snapshotsTopic,
		Value: payload,
	}
	if err := b.producer.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce Kafka state snapshot: %w", err)
	}
	return nil
}

// ReplayEvents applies retained state events through a captured log boundary.
func (b *KafkaBroker) ReplayEvents(ctx context.Context, handler EventHandler) error {
	replayEnd, err := b.logEndOffset(ctx, b.eventsTopic)
	if err != nil {
		return fmt.Errorf("resolve Kafka event replay boundary: %w", err)
	}
	if b.eventsOffset >= replayEnd {
		return nil
	}
	return b.consumeEvents(ctx, replayEnd, handler)
}

// ConsumeEvents tails state events after startup replay.
func (b *KafkaBroker) ConsumeEvents(ctx context.Context, handler EventHandler) error {
	return b.consumeEvents(ctx, -1, handler)
}

func (b *KafkaBroker) consumeEvents(ctx context.Context, replayEnd int64, handler EventHandler) error {
	offset := kgo.NewOffset().At(b.eventsOffset)
	if b.eventsOffset == 0 {
		offset = kgo.NewOffset().AtStart()
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(b.brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			b.eventsTopic: {statePartition: offset},
		}),
	)
	if err != nil {
		return fmt.Errorf("create Kafka state-event consumer: %w", err)
	}
	b.mu.Lock()
	b.consumer = client
	b.mu.Unlock()
	defer func() {
		client.Close()
		b.mu.Lock()
		if b.consumer == client {
			b.consumer = nil
		}
		b.mu.Unlock()
	}()
	for {
		fetches := client.PollFetches(ctx)
		if err := fetchError(ctx, fetches); err != nil {
			return err
		}
		var consumeErr error
		fetches.EachRecord(func(record *kgo.Record) {
			if consumeErr != nil || record.Topic != b.eventsTopic || record.Partition != statePartition || record.Offset < b.eventsOffset || (replayEnd >= 0 && record.Offset >= replayEnd) {
				return
			}
			if err := handler(ctx, record.Offset, append([]byte(nil), record.Value...)); err != nil {
				consumeErr = err
				return
			}
			b.eventsOffset = record.Offset + 1
		})
		if consumeErr != nil {
			return consumeErr
		}
		if replayEnd >= 0 && b.eventsOffset >= replayEnd {
			return nil
		}
	}
}

func (b *KafkaBroker) logEndOffset(ctx context.Context, topicName string) (int64, error) {
	request := kmsg.NewPtrListOffsetsRequest()
	topic := kmsg.NewListOffsetsRequestTopic()
	topic.Topic = topicName
	partition := kmsg.NewListOffsetsRequestTopicPartition()
	partition.Partition = statePartition
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
		if responseTopic.Topic != topicName {
			continue
		}
		for _, responsePartition := range responseTopic.Partitions {
			if responsePartition.Partition != statePartition {
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
	return 0, errors.New("Kafka partition missing from list-offsets response")
}

func fetchError(ctx context.Context, fetches kgo.Fetches) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if fetchErrors := fetches.Errors(); len(fetchErrors) > 0 {
		return fetchErrors[0].Err
	}
	return nil
}

// Close releases the consolidator's Kafka clients.
func (b *KafkaBroker) Close() {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		consumer := b.consumer
		b.consumer = nil
		b.mu.Unlock()
		if consumer != nil {
			consumer.Close()
		}
		b.producer.Close()
	})
}
