package consumer

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

type Option struct {
	Addr  string
	Group string
	Topic string
}

type OnMessage func(message []byte) bool

type MessageHandler interface {
	OnMessage(message []byte) bool
}

type Class struct {
	option         Option
	reader         *kafka.Reader
	messageHandler MessageHandler
	OnMessage      OnMessage
}

func New(o Option) *Class {
	c := &Class{
		option: o,
	}
	c.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{c.option.Addr},
		GroupID:  c.option.Group,
		Topic:    c.option.Topic,
		MinBytes: 1,    // 10KB
		MaxBytes: 10e6, // 10MB
		// ReadBackoffMin:  1000 * time.Millisecond, //消费数据间隔
		ReadLagInterval: 100 * time.Millisecond, //消费数据间隔
		CommitInterval:  time.Second,            // flushes commits to Kafka every second
	})
	return c
}

func (c *Class) reade(addr, group, topic string) error {
	for {
		m, err := c.reader.ReadMessage(context.Background())
		if err != nil {
			break
		}
		// fmt.Printf("consumer group(%s) topic(%s) partition(%d) offset(%d) k(%s) v(%s)\n", group, m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		c.messageHandler.OnMessage(m.Value)
	}
	return nil
}
