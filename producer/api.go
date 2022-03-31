package producer

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type Option struct {
	Addr string
}

type class struct {
	Option
}

// 封装最实用的两种消息生产策略
type Class interface {
	// 数据不保证有序
	//    单分区有序
	//    多分区无序
	// 数据分配到"接收数据量最小"的分区
	// 同时多分区写，高吞吐率
	Write(topic string, value []byte) error
	// 数据按key分区
	// 同一key数据分到同分区，数据有序
	WriteByKey(topic string, key, value []byte) error
}

func New(o Option) Class {
	return &class{
		Option: o,
	}
}

// balancer
//     &kafka.CRC32Balancer{}, //相同key分配到同一分区
//     &kafka.Hash{}, //相同key分配到同一分区
//     &kafka.RoundRobin, //分区轮询
//     &kafka.LeastBytes{}, //数据分配到"接收数据量最小"的分区
func (c *class) write(topic string, key, value []byte, balancer kafka.Balancer) error {
	// make a writer that produces to topic
	w := &kafka.Writer{
		Addr:         kafka.TCP(c.Addr),
		Balancer:     balancer,
		Topic:        topic,
		BatchTimeout: 100 * time.Millisecond, //生产数据间隔
	}
	err := w.WriteMessages(context.Background(), kafka.Message{
		Key:   key,
		Value: value,
	})
	// fmt.Printf("producer topic(%s) k(%s) v(%s)\n", topic, key, value)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil

}

func (c *class) Write(topic string, value []byte) error {
	return c.write(topic, nil, value, &kafka.LeastBytes{})
}

func (c *class) WriteByKey(topic string, key, value []byte) error {
	return c.write(topic, key, value, &kafka.CRC32Balancer{})
}
