package consumer

import (
	"context"
	"testing"

	"github.com/teamseodo/kafka-do/model"
	"github.com/teamseodo/kafka-do/producer"
)

func TestConsumeBatch(t *testing.T) {
	tests := []struct {
		name     string
		messages []model.Message
	}{
		{
			name: "should-got-message",
			messages: []model.Message{
				model.Message("message 1"),
			},
		},
		{
			name: "should-got-all-messages",
			messages: []model.Message{
				model.Message("message 1"), model.Message("message 2"), model.Message("message 3"), model.Message("message 4"), model.Message("message 5"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			producer, err := producer.New("127.0.0.1:9092")
			if err != nil {
				t.Fatal(err)
			}
			defer producer.Close()
			producer.Produce(context.Background(), test.messages, "kafka_do_test")

			consumer, err := New("kafka_do", []string{"kafka_do_test"}, []string{"127.0.0.1:9092"}, false)
			if err != nil {
				t.Fatal(err)
			}
			defer consumer.Close()

			messages := consumer.ConsumeBatch(context.Background(), len(test.messages))

			if len(messages) != len(test.messages) {
				t.Errorf("ConsumeBatch got %d, want %d", len(messages), len(test.messages))
			}
		})
	}
}
