package kafka

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

func TestConsumeChan(t *testing.T) {
	tests := []struct {
		name        string
		messages    [][]byte
		workerCount int
	}{
		{
			name: "should-got-message",
			messages: [][]byte{
				[]byte("message 1"),
			},
			workerCount: 1,
		},
		{
			name: "should-got-all-messages",
			messages: [][]byte{
				[]byte("message 1"), []byte("message 2"), []byte("message 3"), []byte("message 4"), []byte("message 5"),
			},
			workerCount: 1,
		},
		{
			name: "should-work-with-multiple-worker",
			messages: [][]byte{
				[]byte("message 1"), []byte("message 2"), []byte("message 3"), []byte("message 4"), []byte("message 5"),
				[]byte("message 1"), []byte("message 2"), []byte("message 3"), []byte("message 4"), []byte("message 5"),
				[]byte("message 1"), []byte("message 2"), []byte("message 3"), []byte("message 4"), []byte("message 5"),
				[]byte("message 1"), []byte("message 2"), []byte("message 3"), []byte("message 4"), []byte("message 5"),
			},
			workerCount: 5,
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

			_, err := ProduceBatch(ctx, producer, tt.messages, topicName)
			if err != nil {
				t.Errorf("error while writin to Kafka, first error: %s", err)
			}

			outChan := make(chan sarama.ConsumerMessage, 1)
			defer close(outChan)
			var wg sync.WaitGroup
			for i := 0; i < tt.workerCount; i++ {
				wg.Add(1)
				go ConsumeChan(ctx, &wg, consumer, []string{topicName}, outChan)
			}

			var got []sarama.ConsumerMessage
		out:
			for {
				select {
				case msg := <-outChan:
					got = append(got, msg)
					if len(got) >= len(tt.messages) {
						cancel()
						break out
					}
				case <-time.After(15 * time.Second): // maximum wait time for the error.
					break out
				}
			}

			cancel()
			wg.Wait()

			if len(got) != len(tt.messages) {
				t.Errorf("ConsumeChan() = got len %d, want len %d", len(got), len(tt.messages))
			}
		})
	}
}
