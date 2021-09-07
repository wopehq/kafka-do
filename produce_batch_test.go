package kafka

import (
	"context"
	"testing"

	"github.com/Shopify/sarama"
)

func TestProduceBatch(t *testing.T) {
	tests := []struct {
		name     string
		wantErr  bool
		messages interface{}
	}{
		{
			name:     "should-not-work-with-unsupported-type",
			wantErr:  true,
			messages: []int{1, 2, 3},
		},
		{
			name:    "should-work-with-bytes-slice",
			wantErr: false,
			messages: [][]byte{
				[]byte("message 1"), []byte("message 2"), []byte("message 3"),
			},
		},
		{
			name:    "should-work-with-consumer-messages",
			wantErr: false,
			messages: bytesSliceToConsumerMessages([][]byte{
				[]byte("message 1"), []byte("message 2"), []byte("message 3"),
			}),
		},
		{
			name:    "should-work-with-producer-messages",
			wantErr: false,
			messages: bytesSliceToProducerMessages([][]byte{
				[]byte("message 1"), []byte("message 2"), []byte("message 3"),
			}, "responses"),
		},
	}

	client, err := NewProducer([]string{"127.0.0.1:9094"}, 5)
	if err != nil {
		t.Fatalf("error while creating producer. error: %s", err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if errs := ProduceBatch(ctx, client, tt.messages, "responses"); (len(errs) > 0) != tt.wantErr {
				t.Errorf("ProduceBatch() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_produceMessages(t *testing.T) {
	tests := []struct {
		name     string
		messages []*sarama.ProducerMessage
	}{
		{
			name: "returned-count-should-equal-to-sent-message-count",
			messages: bytesSliceToProducerMessages([][]byte{
				[]byte("message 1"), []byte("message 2"), []byte("message 3"),
			}, "responses"),
		},
		{
			name: "returned-count-should-equal-to-sent-message-count",
			messages: bytesSliceToProducerMessages([][]byte{
				[]byte("message 1"),
			}, "responses"),
		},
		{
			name:     "returned-count-should-equal-to-sent-message-count",
			messages: nil,
		},
	}

	client, err := NewProducer([]string{"127.0.0.1:9094"}, 5)
	if err != nil {
		t.Fatalf("error while creating producer. error: %s", err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			errs := produceMessages(ctx, client, tt.messages)
			if len(errs) > 0 {
				t.Errorf("produceMessages() first error = %v", errs[0].Err)
				return
			}
			if want := 0; len(errs) != want {
				t.Errorf("produceMessages() = %v, want %v", len(errs), want)
			}
		})
	}
}
