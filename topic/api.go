package topic

import (
	"fmt"

	"github.com/segmentio/kafka-go"
)

type Option struct {
	Conn *kafka.Conn
}

type class struct {
	Option
}

var this *class

type Class interface {
	Create(topic string, partitions, replicationFactor int) error
	Query(topic string) interface{}
	QueryList() map[string]interface{}
	Delete(topic string) error
}

func New(o Option) Class {
	return &class{
		Option: o,
	}
}

func (c *class) Create(topic string, partitions, replicationFactor int) error {
	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: replicationFactor,
		},
	}
	err := c.Conn.CreateTopics(topicConfigs...)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func (c *class) Query(topic string) interface{} {
	m := c.QueryList()
	if m == nil {
		return nil
	}
	if v, ok := m[topic]; ok {
		return v
	}
	return nil
}

func (c *class) QueryList() map[string]interface{} {
	partitions, err := c.Conn.ReadPartitions()
	if err != nil {
		fmt.Println(err)
		return nil
	}
	m := make(map[string]interface{})
	for _, p := range partitions {
		m[p.Topic] = p.Topic
	}
	if len(m) == 0 {
		return nil
	}
	return m
}

func (c *class) Delete(topic string) error {
	return c.Conn.DeleteTopics(topic)
}
