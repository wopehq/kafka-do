package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

// ProduceChan produces by reading messages from inChan.
// It supports multi-type.
// Supported Types:
//   - bytesSlice: []byte, topic must be set
//   - consumerMessage: sarama.ConsumerMessage, topic must be set
//   - producerMessage: *sarama.ProducerMessage, topic's not needed. Set your topic in the messages.
func ProduceChan(ctx context.Context, wg *sync.WaitGroup, client sarama.SyncProducer, inChan chan interface{}, errChan chan error, topic string) {
	defer wg.Done()
	for {
		select {
		case message := <-inChan:
			var pMessage *sarama.ProducerMessage
			switch m := message.(type) {
			case []byte:
				pMessage = BytesToProducerMessage(m, topic)
			case sarama.ConsumerMessage:
				pMessage = ConsumerMessageToProducerMessage(m, topic)
			case *sarama.ProducerMessage:
				pMessage = m
			default:
				errChan <- ErrNotAllowed
				continue
			}

			err := produceMessage(ctx, client, pMessage)
			if err != nil {
				errChan <- err
			}
		case <-ctx.Done():
			return
		}
	}
}

// produceMessage produces a message. retries 3 times at most.
func produceMessage(ctx context.Context, client sarama.SyncProducer, message *sarama.ProducerMessage) error {
	var err error
	for try := 0; try < 3; try++ {
		_, _, err = client.SendMessage(message)
		if err != nil {
			time.Sleep(3 * time.Second)
			continue
		}
		return nil
	}
	return err
}
