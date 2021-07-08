package kafka

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestConsumeBatch(t *testing.T) {
	tests := []struct {
		name     string
		messages [][]byte
	}{
		{
			name: "should-got-message",
			messages: [][]byte{
				[]byte("message 1"),
			},
		},
		{
			name: "should-got-all-messages",
			messages: [][]byte{
				[]byte("message 1"), []byte("message 2"), []byte("message 3"), []byte("message 4"), []byte("message 5"),
			},
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
		topicName := fmt.Sprintf("requests-test-%d", r) // to seperate tests, create random topics.

		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			err := ProduceBatch(ctx, producer, tt.messages, topicName)
			if err != nil {
				t.Errorf("error while writing to Kafka, error: %s", err)
			}

			got := ConsumeBatch(ctx, consumer, []string{topicName}, len(tt.messages))
			if err != nil {
				t.Errorf("error while reading from Kafka, error: %s", err)
			}

			if len(got) != len(tt.messages) {
				t.Errorf("ConsumeBatch() = got len %d, want len %d", len(got), len(tt.messages))
			}
		})
	}
}
