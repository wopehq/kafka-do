package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer struct {
	client *kgo.Client
}

func NewProducer(maxBytes int32, brokers ...string) (*Producer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ProducerBatchMaxBytes(maxBytes),
	)
	if err != nil {
		return nil, err
	}

	return &Producer{
		client: cl,
	}, nil
}

func (p *Producer) Produce(ctx context.Context, messages []Message, topic string) kgo.ProduceResults {
	var records []*kgo.Record

	for _, message := range messages {
		records = append(records, &kgo.Record{Topic: topic, Value: message})
	}

	return p.client.ProduceSync(ctx, records...)
}

func (p *Producer) Close() {
	p.client.Close()
}
