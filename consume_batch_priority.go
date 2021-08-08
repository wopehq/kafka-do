package kafka

import (
	"context"
	"log"
	"time"

	"github.com/Shopify/sarama"
)

// ConsumeBatchPriority takes messages by implementing Priority structure.
// Loop over topics and tries to take message. If there is any message consumed
// The first item in the "topics" slice is considered the highest topic.
// Returns the messages when as much as batchCount is arrived or time exceed,
// (if there is no message on any priority stage, time will may be exceed.)
func ConsumeBatchPriority(ctx context.Context, client sarama.ConsumerGroup, topics []string, batchCount int) ([]sarama.ConsumerMessage, string) {
	if batchCount == 0 {
		return nil, ""
	}

	consumer := newBatchConsumer()
	defer close(consumer.done)
	consumer.batchCount = batchCount
	consumer.client = client

	for i := 0; ; { // Endless loop. Runs until any message exists.
		consumer.topics = []string{topics[i]}

		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		go consumer.consume(cctx)
	out:
		for {
			select {
			case <-time.After(10 * time.Second):
				cancel()
				<-consumer.done
				if len(consumer.messages) == 0 {
					break out // if there is no message, go to next priorty.
				}
			case <-consumer.done:
			}
			log.Printf("consumed %d messages from %s", len(consumer.messages), topics[i])
			return consumer.messages, topics[i]
		}

		i++
		i = i % len(topics)
	}
}
