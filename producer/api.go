package producer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type class struct {
	writer *kafka.Writer
	once   sync.Once
}

var this = &class{}

// 封装最实用的两种消息生产策略
// type Class interface {
// 	Write(addr, topic string, value []byte) error
// 	WriteByKey(addr, topic string, key, value []byte) error
// }

// func (c *class) write(key, value []byte) error {
// 	err := c.writer.WriteMessages(context.Background(), kafka.Message{
// 		Key:   key,
// 		Value: value,
// 	})
// 	// fmt.Printf("producer topic(%s) k(%s) v(%s)\n", topic, key, value)
// 	if err != nil {
// 		fmt.Println(err)
// 		return err
// 	}
// 	return nil
// }

// 数据不保证有序
//    单分区有序
//    多分区无序
// 数据分配到"接收数据量最小"的分区
// 同时多分区写，高吞吐率
func Write(addr, topic string, value []byte) error {
	this.once.Do(func() {
		// make a writer that produces to topic
		this.writer = &kafka.Writer{
			Addr:         kafka.TCP(addr),
			Balancer:     &kafka.LeastBytes{},
			Topic:        topic,
			BatchTimeout: 100 * time.Millisecond, //生产数据间隔
		}
	})
	err := this.writer.WriteMessages(context.Background(), kafka.Message{Value: value})
	// fmt.Printf("producer topic(%s) k(%s) v(%s)\n", topic, key, value)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

// 数据按key分区
// 同一key数据分到同分区，数据有序
func WriteByKey(addr, topic string, key, value []byte) error {
	this.once.Do(func() {
		// make a writer that produces to topic
		this.writer = &kafka.Writer{
			Addr:         kafka.TCP(addr),
			Balancer:     &kafka.CRC32Balancer{},
			Topic:        topic,
			BatchTimeout: 100 * time.Millisecond, //生产数据间隔
		}
	})
	err := this.writer.WriteMessages(context.Background(), kafka.Message{Key: key, Value: value})
	// fmt.Printf("producer topic(%s) k(%s) v(%s)\n", topic, key, value)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}
