package producer

import (
	"context"

	"github.com/teamseodo/kafka-do/model"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer struct {
	client *kgo.Client
}

func New(brokers ...string) (*Producer, error) {
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

func (p *Producer) Produce(ctx context.Context, messages []model.Message, topic string) kgo.ProduceResults {
	var records []*kgo.Record

	for _, message := range messages {
		records = append(records, &kgo.Record{Topic: topic, Value: message})
	}

	return p.client.ProduceSync(ctx, records...)
}

func (p *Producer) Close() {
	p.client.Close()
}
