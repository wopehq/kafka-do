package kafka

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Message []byte

type Consumer struct {
	client *kgo.Client
}

func NewConsumer(groupName string, topics []string, brokers []string) (*Consumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupName),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		client: cl,
	}, nil
}

func (c *Consumer) ConsumeBatch(ctx context.Context, batchSize int) ([]Message, []kgo.FetchError) {
	var messages []Message
	var errs []kgo.FetchError

consume:
	for {
		timeout, cancel := context.WithTimeout(ctx, time.Second*20)
		defer cancel()

		for {
			fetches := c.client.PollFetches(timeout)
			errs = fetches.Errors()

			if timeout.Err() != nil {
				cancel()
				break consume
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				record := iter.Next()

				c.client.CommitRecords(ctx, record)
				messages = append(messages, record.Value)
			}

			if len(messages) >= batchSize || len(errs) > 0 {
				cancel()
				break consume
			}
		}

	}

	if len(messages) == 0 {
		goto consume
	}

	return messages, errs
}

func (c *Consumer) Close() {
	c.client.Close()
}
