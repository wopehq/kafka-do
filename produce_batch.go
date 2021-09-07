package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/Shopify/sarama"
)

var ErrNotAllowedType = errors.New("not allowed type")
var ErrNotAllowedTypeProducerError = sarama.ProducerError{
	Msg: nil,
	Err: ErrNotAllowedType,
}
var ErrCantProduce = errors.New("cant produce the message")

// ProduceBatch produces given data with the client.
// It supports multi-type.
// Supported Types:
//   - bytesSlice: [][]byte, topic must be set
//   - consumerMessages: []sarama.ConsumerMessage, topic must be set
//   - producerMessages: []*sarama.ProducerMessage, topic's not needed. Set your topic in the messages.
//   - producerErrors: sarama.ProducerErrors, topic isn't needed. we expect topic to be already set in producerError.Msg
func ProduceBatch(ctx context.Context, client sarama.SyncProducer, messages interface{}, topic string) sarama.ProducerErrors {
	switch m := messages.(type) {
	case [][]byte:
		return produceMessages(ctx, client, bytesSliceToProducerMessages(m, topic))
	case []sarama.ConsumerMessage:
		return produceMessages(ctx, client, consumerMessagesToProducerMessages(m, topic))
	case []*sarama.ProducerMessage:
		return produceMessages(ctx, client, m)
	case sarama.ProducerErrors:
		return produceMessages(ctx, client, producerErrorsToProducerMessages(m, topic))
	default:
		return sarama.ProducerErrors{&ErrNotAllowedTypeProducerError}
	}
}

// produceMessages produces messages. retries 3 times at most.
// returns produced messages count.
func produceMessages(ctx context.Context, client sarama.SyncProducer, messages []*sarama.ProducerMessage) sarama.ProducerErrors {
	var err error
	var errs sarama.ProducerErrors
	for try := 0; try < 3; try++ {
		err = client.SendMessages(messages)
		messages = []*sarama.ProducerMessage{} // clear given messages.

		if err == nil {
			break
		}

		errs = err.(sarama.ProducerErrors)
		for _, pErr := range errs {
			messages = append(messages, pErr.Msg)
		}
	}

	if err != nil { // overwrite error message to show total unproduced messages count.
		err = fmt.Errorf("failed to deliver %d messages, last error: %w", len(messages), err)
	}

	return errs
}
