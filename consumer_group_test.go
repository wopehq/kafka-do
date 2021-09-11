package kafka

import (
	"testing"
)

func TestNewConsumerGroup(t *testing.T) {
	type args struct {
		brokers []string
		groupId string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "should-work",
			args: args{
				brokers: []string{"127.0.0.1:9094"},
				groupId: "kafka-do",
			},
			wantErr: false,
		},
		{
			name: "should-not-work-without-groupId",
			args: args{
				brokers: []string{"127.0.0.1:9094"},
				groupId: "",
			},
			wantErr: true,
		},
		{
			name: "should-not-work-without-brokers",
			args: args{
				brokers: []string{},
				groupId: "kafka-do",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewConsumerGroup(tt.args.brokers, tt.args.groupId)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConsumerGroup() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				_ = got.Close()
			}
		})
	}
}
