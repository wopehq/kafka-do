package kafka

import (
	"context"
	"testing"
)

func TestProduce(t *testing.T) {
	tests := []struct {
		name     string
		messages []Message
	}{
		{
			name: "should-got-message",
			messages: []Message{
				Message("message 1"),
			},
		},
		{
			name: "should-got-all-messages",
			messages: []Message{
				Message("message 1"), Message("message 2"), Message("message 3"), Message("message 4"), Message("message 5"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			producer, err := NewProducer("127.0.0.1:9092")
			if err != nil {
				t.Fatal(err)
			}
			defer producer.Close()

			producer.Produce(context.Background(), test.messages, "kafka_do_test")

			consumer, err := NewConsumer("kafka_do", []string{"kafka_do_test"}, []string{"127.0.0.1:9092"}, false)
			if err != nil {
				t.Fatal(err)
			}
			defer consumer.Close()

			messages := consumer.ConsumeBatch(context.Background(), len(test.messages))

			if len(messages) != len(test.messages) {
				t.Errorf("Produce got %d, want %d", len(messages), len(test.messages))
			}
		})
	}
}
