package kafka

import "github.com/Shopify/sarama"

// BytesSliceToProducerMessages converts given bytes to producer messages.
func BytesSliceToProducerMessages(inputs [][]byte, topic string) []*sarama.ProducerMessage {
	var ms []*sarama.ProducerMessage

	for _, i := range inputs {
		ms = append(ms, BytesToProducerMessage(i, topic))
	}

	return ms
}

// BytesSliceToConsumerMessages converts given bytes to consumer messages.
func BytesSliceToConsumerMessages(inputs [][]byte) []sarama.ConsumerMessage {
	var ms []sarama.ConsumerMessage

	for _, i := range inputs {
		ms = append(ms, BytesToConsumerMessage(i))
	}

	return ms
}

// BytesToProducerMessage converts given bytes to producer message.
func BytesToProducerMessage(i []byte, topic string) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(i),
	}
}

// BytesToConsumerMessage converts given bytes to consumer message.
func BytesToConsumerMessage(i []byte) sarama.ConsumerMessage {
	return sarama.ConsumerMessage{
		Value: i,
	}
}

// ConsumerMessagesToBytesSlice converts given messages to bytes.
func ConsumerMessagesToBytesSlice(messages []sarama.ConsumerMessage) [][]byte {
	var r [][]byte

	for _, m := range messages {
		r = append(r, m.Value)
	}

	return r
}

func ConsumerMessagesToProducerMessages(messages []sarama.ConsumerMessage, topic string) []*sarama.ProducerMessage {
	var ms []*sarama.ProducerMessage

	for _, m := range messages {
		ms = append(ms, ConsumerMessageToProducerMessage(m, topic))
	}

	return ms
}

func ConsumerMessageToProducerMessage(m sarama.ConsumerMessage, topic string) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(m.Value),
	}
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
