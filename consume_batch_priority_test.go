package kafka

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestConsumeBatchPriority(t *testing.T) {
	tests := []struct {
		name           string
		messageCount   int
		topicToProduce string
		topics         []string
		batchCount     int
	}{
		{
			name:           "should-got-all-messages-from-high",
			messageCount:   3000,
			batchCount:     3000,
			topicToProduce: "high",
			topics:         []string{"high", "med", "low"},
		},
		{
			name:           "should-got-all-messages-from-med",
			messageCount:   3000,
			batchCount:     3000,
			topicToProduce: "med",
			topics:         []string{"high", "med", "low"},
		},
		{
			name:           "should-got-all-messages-from-low",
			messageCount:   3000,
			batchCount:     3000,
			topicToProduce: "low",
			topics:         []string{"high", "med", "low"},
		},
		{
			name:           "should-got-all-messages-from-high-lower-then-batch-count",
			messageCount:   1000,
			batchCount:     3000,
			topicToProduce: "high",
			topics:         []string{"high", "med", "low"},
		},
	}

	producer, err := NewProducer([]string{"127.0.0.1:9094"}, 5)
	if err != nil {
		t.Fatalf("error while creating consumer group, error: %s", err)
	}

	consumer, err := NewConsumerGroup([]string{"127.0.0.1:9094"}, "kafka-do")
	if err != nil {
		t.Fatalf("error while creating consumer group, error: %s", err)
	}

	for _, tt := range tests {
		rand.Seed(time.Now().UnixNano())
		r := rand.Intn(1000) + 1000

		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			topicName := fmt.Sprintf("%s-%d", tt.topicToProduce, r)
			_, err := ProduceBatch(ctx, producer, randomMessages(tt.messageCount), topicName)
			if err != nil {
				t.Fatalf("error while producing batch to %s", tt.topicToProduce)
			}

			messages, fromTopic := ConsumeBatchPriority(ctx, consumer, addValueToStrings(tt.topics, r), tt.batchCount)
			if fromTopic != topicName {
				t.Errorf("ConsumeBatchPriority() = got fromTopic %s, want fromTopic %s", topicName, fromTopic)
			}
			if got := len(messages); got != tt.messageCount {
				t.Errorf("ConsumeBatchPriority() = got len %d, want len %d", len(messages), tt.messageCount)
			}
		})
	}
}

func randomMessages(size int) [][]byte {
	var ms [][]byte
	for i := 0; i < size; i++ {
		ms = append(ms, []byte("message"))
	}
	return ms
}

func addValueToStrings(ss []string, v interface{}) []string {
	for i, s := range ss {
		ss[i] = fmt.Sprintf("%s-%v", s, v)
	}
	return ss
}
