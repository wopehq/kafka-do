package kafka

import (
	"errors"
	"fmt"

	"github.com/Shopify/sarama"
)

var ErrNotAllowed = errors.New("input is not allowed")

// NewProducer returns a sarama.SyncProducer.
func NewProducer(brokers []string, maxMegabytes int) (sarama.SyncProducer, error) {
	if maxMegabytes <= 0 {
		return nil, fmt.Errorf("%w, maxMegabytes should be at least 1", ErrNotAllowed)
	}
	if len(brokers) == 0 {
		return nil, fmt.Errorf("%w, brokers count should be at least 1", ErrNotAllowed)
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Idempotent = true
	config.Producer.Compression = sarama.CompressionGZIP
	config.Producer.MaxMessageBytes = maxMegabytes * 1000000
	config.Producer.Retry.Max = 3
	config.Net.MaxOpenRequests = 1

	return sarama.NewSyncProducer(brokers, config)
}
