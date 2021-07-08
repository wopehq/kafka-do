package kafka

import (
	"testing"
)

func TestNewProducer(t *testing.T) {
	tests := []struct {
		name         string
		brokers      []string
		maxMegabytes int
		wantErr      bool
	}{
		{
			name:         "should-not-work-without-brokers",
			brokers:      nil,
			maxMegabytes: 1,
			wantErr:      true,
		},
		{
			name:         "should-not-work-without-maxMegabytes-0",
			brokers:      []string{"127.0.0.1:9094"},
			maxMegabytes: 0,
			wantErr:      true,
		},
		{
			name:         "should-not-work-without-maxMegabytes-negative",
			brokers:      []string{"127.0.0.1:9094"},
			maxMegabytes: -1,
			wantErr:      true,
		},
		{
			name:         "should-work",
			brokers:      []string{"127.0.0.1:9094"},
			maxMegabytes: 1,
			wantErr:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewProducer(tt.brokers, tt.maxMegabytes)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewProducer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				defer got.Close()
			}
		})
	}
}
