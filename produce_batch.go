package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/Shopify/sarama"
)

var ErrNotAllowedType = errors.New("not allowed type")
var ErrCantProduce = errors.New("cant produce the message")

// ProduceBatch produces given data with the client.
// It supports multi-type.
// Supported Types:
//   - bytesSlice: [][]byte, topic must be set
//   - consumerMessages: []sarama.ConsumerMessage, topic must be set
//   - producerMessages: []*sarama.ProducerMessage, topic's not needed. Set your topic in the messages.
func ProduceBatch(ctx context.Context, client sarama.SyncProducer, messages interface{}, topic string) (int, error) {
	switch m := messages.(type) {
	case [][]byte:
		return produceMessages(ctx, client, bytesSliceToProducerMessages(m, topic))
	case []sarama.ConsumerMessage:
		return produceMessages(ctx, client, consumerMessagesToProducerMessages(m, topic))
	case []*sarama.ProducerMessage:
		return produceMessages(ctx, client, m)
	default:
		return 0, ErrNotAllowedType
	}
}

// produceMessages produces messages. retries 3 times at most.
// returns produced messages count.
func produceMessages(ctx context.Context, client sarama.SyncProducer, messages []*sarama.ProducerMessage) (int, error) {
	wantProduceCount := len(messages)

	var err error
	for try := 0; try < 3; try++ {
		err = client.SendMessages(messages)
		messages = []*sarama.ProducerMessage{} // clear given messages.
		if err != nil {
			producerErrors := err.(sarama.ProducerErrors)
			for _, pErr := range producerErrors {
				messages = append(messages, pErr.Msg)
			}
			continue
		}
		break
	}

	if err != nil { // overwrite error message to show total unproduced messages count.
		err = fmt.Errorf("failed to deliver %d messages, last error: %w", len(messages), err)
	}

	return wantProduceCount - len(messages), err
}
