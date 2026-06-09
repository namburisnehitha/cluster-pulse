package kafka

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokerAddr, topic, groupID string) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddr},
		Topic:   topic,
		GroupID: groupID,
	})
	return &Consumer{reader: reader}
}

func (c *Consumer) Consume(ctx context.Context) (PodEvent, error) {

	var event PodEvent

	msg, err := c.reader.ReadMessage(ctx)

	if err != nil {
		return PodEvent{}, err
	}

	err = json.Unmarshal(msg.Value, &event)

	if err != nil {
		return PodEvent{}, err
	}

	return event, nil

}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
