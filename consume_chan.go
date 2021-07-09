package kafka

import (
	"context"
	"log"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
)

// ConsumeChan starts consuming messages and write messages to outChan.
func ConsumeChan(ctx context.Context, wg *sync.WaitGroup, client sarama.ConsumerGroup, topics []string, outChan chan sarama.ConsumerMessage) {
	defer wg.Done()
	consumer := newChanConsumer()

	consumer.messageChan = outChan
	consumer.topics = topics
	consumer.client = client

	consumer.consume(ctx)
}

type chanConsumer struct {
	topics      []string
	client      sarama.ConsumerGroup
	messageChan chan sarama.ConsumerMessage
}

func newChanConsumer() *chanConsumer {
	consumer := &chanConsumer{}
	return consumer
}

func (c *chanConsumer) consume(ctx context.Context) {
	err := c.client.Consume(ctx, c.topics, c)
	if err != nil {
		log.Printf("consume from %s topic error: %s", strings.Join(c.topics, ", "), err.Error())
	}
}

func (c *chanConsumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *chanConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *chanConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		if message.Value == nil || len(message.Value) == 0 {
			continue
		}

		c.messageChan <- *message
		session.MarkMessage(message, "")
	}

	return nil
}
