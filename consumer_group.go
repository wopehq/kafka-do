package kafka

import (
	"errors"

	"github.com/Shopify/sarama"
)

var ErrNoGroupID = errors.New("groupID is not defined")
var ErrNoBrokers = errors.New("brokers is not defined")

// NewConsumerGroup returns a sarama.ConsumerGroup.
func NewConsumerGroup(brokers []string, groupId string) (sarama.ConsumerGroup, error) {
	if groupId == "" {
		return nil, ErrNoGroupID
	}
	if len(brokers) == 0 {
		return nil, ErrNoBrokers
	}

	config := sarama.NewConfig()
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	return sarama.NewConsumerGroup(brokers, groupId, config)
}
