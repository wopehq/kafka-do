package kafka

import (
	"context"
	"testing"
	"time"
)

func TestProduceChan(t *testing.T) {
	tests := []struct {
		name     string
		wantErr  bool
		messages []interface{}
	}{
		{
			name:     "should-not-work-with-unsupported-type",
			wantErr:  true,
			messages: intSliceAsInterfaceSlice([]int{1, 2, 3}),
		},
		{
			name:    "should-work-with-bytes",
			wantErr: false,
			messages: bytesSliceAsInterfaceSlice([][]byte{
				[]byte("message 1"), []byte("message 2"), []byte("message 3"),
			}),
		},
		{
			name:    "should-work-with-consumer-message",
			wantErr: false,
			messages: consumerMessagesAsInterfaceSlice([][]byte{
				[]byte("message 1"), []byte("message 2"), []byte("message 3"),
			}),
		},
		{
			name:    "should-work-with-producer-message",
			wantErr: false,
			messages: producerMessagesAsInterfaceSlice([][]byte{
				[]byte("message 1"), []byte("message 2"), []byte("message 3"),
			}),
		},
	}

	client, err := NewProducer([]string{"127.0.0.1:9094"}, 5)
	if err != nil {
		t.Fatalf("error while creating producer. error: %s", err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inChan := make(chan interface{}, 10)
			errChan := make(chan error, 10)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go ProduceChan(ctx, client, inChan, errChan, "responses")
			for _, m := range tt.messages {
				inChan <- m
			}

			select {
			case err := <-errChan:
				if (err != nil) != tt.wantErr {
					t.Errorf("ProduceChan() error = %v, wantErr %v", err, tt.wantErr)
				}
			case <-time.After(5 * time.Second): // maximum wait time for the error.
			}
		})
	}
}