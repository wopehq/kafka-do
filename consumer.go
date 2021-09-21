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
		kgo.GroupProtocol("roundrobin"),
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
		timeout, cancel := context.WithTimeout(ctx, time.Minute*1)
		defer cancel()
		fetches := c.client.PollFetches(timeout)
		errs = fetches.Errors()

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			c.client.CommitRecords(ctx, record)
			messages = append(messages, record.Value)

			if timeout.Err() != nil && len(messages) > 0 {
				break consume
			}

			if len(messages) >= batchSize || len(errs) > 0 {
				break consume
			}
		}
	}

	return messages, errs
}

func (c *Consumer) Close() {
	c.client.Close()
}
