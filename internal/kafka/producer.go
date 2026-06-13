package kafka

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

func NewProducer(brokers []string, topic string) *Producer {
	writer := &kafka.Writer{
		Addr:  kafka.TCP(brokers...),
		Topic: topic,
	}
	return &Producer{writer: writer}
}

func (p *Producer) Publish(ctx context.Context, event PodEvent) error {

	data, err := json.Marshal(event)

	if err != nil {
		return err
	}

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Value: data,
	})

	if err != nil {
		return err
	}

	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
