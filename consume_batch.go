package kafka

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

// ConsumeBatch takes messages as much as batchCount by using given sarama.ConsumerGroup.
// Re-tries every 5 seconds until the success. Returns as sarama.ConsumerMessage slice.
// Runs 1 minutes at most. (if there is no message, time will may be exceed.)
func ConsumeBatch(ctx context.Context, client sarama.ConsumerGroup, topics []string, batchCount int) []sarama.ConsumerMessage {
	consumer := newBatchConsumer()
	defer close(consumer.done)
	consumer.batchCount = batchCount
	consumer.topics = topics
	consumer.client = client

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go consumer.consume(cctx)
	for {
		select {
		case <-time.After(1 * time.Minute):
			if len(consumer.messages) == 0 {
				continue
			}
			cancel()
			<-consumer.done
		case <-consumer.done:
		}
		log.Printf("consumed %d messages from %s", len(consumer.messages), strings.Join(topics, ", "))
		return consumer.messages
	}
}

type batchConsumer struct {
	batchCount int
	done       chan bool
	topics     []string
	rwlock     sync.RWMutex
	client     sarama.ConsumerGroup
	messages   []sarama.ConsumerMessage
}

func newBatchConsumer() *batchConsumer {
	consumer := &batchConsumer{}
	consumer.done = make(chan bool, 1)
	return consumer
}

func (c *batchConsumer) consume(ctx context.Context) {
	for {
		err := c.client.Consume(ctx, c.topics, c)
		if ctx.Err() != nil {
			log.Printf("context is canceled")
			c.done <- true
			return
		}

		if c.batchCount <= len(c.messages) {
			c.done <- true
			return
		}
		if err != nil {
			log.Printf("consume from %s topic error: %s", strings.Join(c.topics, ", "), err.Error())
		}
		select {
		case <-ctx.Done():
			log.Printf("context is canceled")
			c.done <- true
			return
		case <-time.After(5 * time.Second):
		}
	}
}

func (c *batchConsumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *batchConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *batchConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		if message.Value == nil || len(message.Value) == 0 {
			continue
		}

		c.rwlock.Lock()
		c.messages = append(c.messages, *message)
		session.MarkMessage(message, "")
		if c.batchCount <= len(c.messages) {
			c.rwlock.Unlock()
			break
		}
		c.rwlock.Unlock()
	}

	return nil
}
