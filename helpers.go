package kafka

import "github.com/Shopify/sarama"

// bytesSliceToProducerMessages converts given bytes to producer messages.
func bytesSliceToProducerMessages(inputs [][]byte, topic string) []*sarama.ProducerMessage {
	var ms []*sarama.ProducerMessage

	for _, i := range inputs {
		ms = append(ms, bytesToProducerMessage(i, topic))
	}

	return ms
}

// bytesSliceToConsumerMessages converts given bytes to consumer messages.
func bytesSliceToConsumerMessages(inputs [][]byte) []sarama.ConsumerMessage {
	var ms []sarama.ConsumerMessage

	for _, i := range inputs {
		ms = append(ms, bytesToConsumerMessage(i))
	}

	return ms
}

// bytesToProducerMessage converts given bytes to producer message.
func bytesToProducerMessage(i []byte, topic string) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(i),
	}
}

// bytesToConsumerMessage converts given bytes to consumer message.
func bytesToConsumerMessage(i []byte) sarama.ConsumerMessage {
	return sarama.ConsumerMessage{
		Value: i,
	}
}

func consumerMessagesToProducerMessages(messages []sarama.ConsumerMessage, topic string) []*sarama.ProducerMessage {
	var ms []*sarama.ProducerMessage

	for _, m := range messages {
		ms = append(ms, consumerMessageToProducerMessage(m, topic))
	}

	return ms
}

func consumerMessageToProducerMessage(m sarama.ConsumerMessage, topic string) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(m.Value),
	}
}

func producerErrorsToProducerMessages(messages sarama.ProducerErrors, topic string) []*sarama.ProducerMessage {
	var ms []*sarama.ProducerMessage

	for _, m := range messages {
		ms = append(ms, producerErrorToProducerMessage(m, topic))
	}

	return ms
}

func producerErrorToProducerMessage(m *sarama.ProducerError, topic string) *sarama.ProducerMessage {
	return m.Msg
}

func intSliceAsInterfaceSlice(values []int) []interface{} {
	iValues := make([]interface{}, len(values))
	for i, v := range values {
		iValues[i] = v
	}
	return iValues
}

func bytesSliceAsInterfaceSlice(values [][]byte) []interface{} {
	iValues := make([]interface{}, len(values))
	for i, v := range values {
		iValues[i] = v
	}
	return iValues
}

func consumerMessagesAsInterfaceSlice(values [][]byte) []interface{} {
	iValues := make([]interface{}, len(values))
	for i, v := range values {
		iValues[i] = sarama.ConsumerMessage{
			Value: v,
		}
	}
	return iValues
}

func producerMessagesAsInterfaceSlice(values [][]byte) []interface{} {
	iValues := make([]interface{}, len(values))
	for i, v := range values {
		iValues[i] = &sarama.ProducerMessage{
			Value: sarama.ByteEncoder(v),
			Topic: "responses",
		}
	}
	return iValues
}
