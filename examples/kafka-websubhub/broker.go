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
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
)

const eventsPartition int32 = 0

type eventConsumer func(context.Context, stateEvent) error
type updateConsumer func(context.Context, string, updateEvent) error

type messageBroker interface {
	PublishEvent(context.Context, stateEvent) error
	PublishUpdate(context.Context, updateEvent) error
	PublishDeadLetter(context.Context, deadLetter) error
	ReplayEvents(context.Context, eventConsumer) error
	ConsumeEvents(context.Context, eventConsumer) error
	ConsumeUpdates(context.Context, updateConsumer) error
	Close()
}

type kafkaBrokerOptions struct {
	brokers         []string
	eventsTopic     string
	updatesTopic    string
	deadLetterTopic string
	deliveryGroup   string
}

type kafkaBroker struct {
	producer        *kgo.Client
	eventsClient    *kgo.Client
	updatesClient   *kgo.Client
	eventsTopic     string
	updatesTopic    string
	deadLetterTopic string
	eventsOffset    int64
}

func newKafkaBroker(ctx context.Context, options kafkaBrokerOptions) (*kafkaBroker, error) {
	if len(options.brokers) == 0 {
		return nil, errors.New("at least one Kafka broker is required")
	}
	if options.eventsTopic == "" || options.updatesTopic == "" || options.deadLetterTopic == "" {
		return nil, errors.New("Kafka topic names must not be empty")
	}
	if options.deliveryGroup == "" {
		return nil, errors.New("Kafka delivery consumer group must not be empty")
	}

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(options.brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
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
	updatesClient, err := kgo.NewClient(
		kgo.SeedBrokers(options.brokers...),
		kgo.ConsumerGroup(options.deliveryGroup),
		kgo.ConsumeTopics(options.updatesTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
	)
	if err != nil {
		eventsClient.Close()
		producer.Close()
		return nil, fmt.Errorf("create Kafka update consumer: %w", err)
	}

	return &kafkaBroker{
		producer:        producer,
		eventsClient:    eventsClient,
		updatesClient:   updatesClient,
		eventsTopic:     options.eventsTopic,
		updatesTopic:    options.updatesTopic,
		deadLetterTopic: options.deadLetterTopic,
	}, nil
}

func (b *kafkaBroker) PublishEvent(ctx context.Context, event stateEvent) error {
	return b.produceJSON(ctx, b.eventsTopic, event)
}

func (b *kafkaBroker) PublishUpdate(ctx context.Context, update updateEvent) error {
	return b.produceJSON(ctx, b.updatesTopic, update)
}

func (b *kafkaBroker) PublishDeadLetter(ctx context.Context, letter deadLetter) error {
	return b.produceJSON(ctx, b.deadLetterTopic, letter)
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
				event, err := decodeEvent(record.Value)
				if err != nil {
					applyErr = fmt.Errorf("decode event at offset %d: %w", record.Offset, err)
					return
				}
				if err := apply(ctx, event); err != nil {
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
			event, err := decodeEvent(record.Value)
			if err != nil {
				applyErr = fmt.Errorf("decode event at offset %d: %w", record.Offset, err)
				return
			}
			if err := apply(ctx, event); err != nil {
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

func (b *kafkaBroker) ConsumeUpdates(ctx context.Context, consume updateConsumer) error {
	for {
		fetches := b.updatesClient.PollFetches(ctx)
		if err := firstFetchError(ctx, fetches); err != nil {
			return fmt.Errorf("consume Kafka updates: %w", err)
		}
		var consumeErr error
		fetches.EachRecord(func(record *kgo.Record) {
			if consumeErr != nil {
				return
			}
			var update updateEvent
			if err := json.Unmarshal(record.Value, &update); err != nil {
				consumeErr = fmt.Errorf("decode update record at offset %d: %w", record.Offset, err)
				return
			}
			recordID := record.Topic + "-" + strconv.FormatInt(int64(record.Partition), 10) + "-" + strconv.FormatInt(record.Offset, 10)
			if err := consume(ctx, recordID, update); err != nil {
				consumeErr = err
				return
			}
			if err := b.updatesClient.CommitRecords(ctx, record); err != nil {
				consumeErr = fmt.Errorf("commit update offset %d: %w", record.Offset, err)
			}
		})
		b.updatesClient.AllowRebalance()
		if consumeErr != nil {
			return consumeErr
		}
	}
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

func decodeEvent(payload []byte) (stateEvent, error) {
	var event stateEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return stateEvent{}, err
	}
	return event, nil
}

func (b *kafkaBroker) Close() {
	b.updatesClient.Close()
	b.eventsClient.Close()
	b.producer.Close()
}
