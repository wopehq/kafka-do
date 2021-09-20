package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer struct {
	client *kgo.Client
}

func NewProducer(brokers ...string) (*Producer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
	)
	if err != nil {
		return nil, err
	}

	return &Producer{
		client: cl,
	}, nil
}

func (p *Producer) Produce(ctx context.Context, messages []Message, topic string) {
	var records []*kgo.Record

	for _, message := range messages {
		records = append(records, &kgo.Record{Topic: topic, Value: message})
	}

	results := p.client.ProduceSync(ctx, records...)
	for _, result := range results {
		fmt.Println(result)
	}
}
