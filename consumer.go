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

func (c *Consumer) ConsumeBatch(ctx context.Context, batchSize int) []Message {
	var messages []Message

	for batchSize > 0 {
		timeout, cancel := context.WithTimeout(ctx, time.Minute*1)
		defer cancel()

		fetches := c.client.PollRecords(timeout, batchSize)

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			messages = append(messages, record.Value)
		}

		batchSize = batchSize - len(messages)

		if ctx.Err() != nil {
			break
		}
	}
	c.client.CommitUncommittedOffsets(ctx)

	return messages
}

func (c *Consumer) Close() {
	c.client.Close()
}
