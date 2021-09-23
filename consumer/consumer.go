package consumer

import (
	"context"
	"os"
	"time"

	"github.com/teamseodo/kafka-do/constants"
	"github.com/teamseodo/kafka-do/model"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Consumer struct {
	client *kgo.Client
}

func New(groupName string, topics []string, brokers []string, logger bool) (*Consumer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupName),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.GroupProtocol(constants.GroupProtocol),
	}

	if logger {
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)))
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		client: cl,
	}, nil
}

func (c *Consumer) ConsumeBatch(ctx context.Context, batchSize int) []model.Message {
	var messages []model.Message

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
