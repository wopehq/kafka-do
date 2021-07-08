package kafka

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
)

// ProduceChan produces by reading messages from inChan.
// It supports multi-type.
// Supported Types:
//   - bytesSlice: []byte, topic must be set
//   - consumerMessage: sarama.ConsumerMessage, topic must be set
//   - producerMessage: *sarama.ProducerMessage, topic's not needed. Set your topic in the messages.
func ProduceChan(ctx context.Context, client sarama.SyncProducer, inChan chan interface{}, errChan chan error, topic string) {
	for {
		message := <-inChan
		go produceMessage(ctx, client, message, errChan, topic)
	}
}

// produceMessage produces a message. retries 3 times at most.
// returns produced messages count.
func produceMessage(ctx context.Context, client sarama.SyncProducer, message interface{}, errChan chan error, topic string) {
	var producerMessage *sarama.ProducerMessage

	switch m := message.(type) {
	case []byte:
		producerMessage = BytesToProducerMessage(m, topic)
	case sarama.ConsumerMessage:
		producerMessage = ConsumerMessageToProducerMessage(m, topic)
	case *sarama.ProducerMessage:
		producerMessage = m
	default:
		errChan <- ErrNotAllowed
		return
	}

	for try := 0; try < 3; try++ {
		_, _, err := client.SendMessage(producerMessage)
		if err != nil {
			time.Sleep(3 * time.Second)
			continue
		}
		return
	}

	errChan <- ErrCantProduce
}
