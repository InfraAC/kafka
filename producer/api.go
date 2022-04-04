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

// 封装最常用的两种消息生产策略
// type Class interface {
// Write(addr, topic string, value []byte) error
// WriteByKey(addr, topic string, key, value []byte) error
// }

func (c *class) write(addr, topic string, key, value []byte, balancer kafka.Balancer) error {
	this.once.Do(func() {
		// make a writer that produces to topic
		this.writer = &kafka.Writer{
			Addr:         kafka.TCP(addr),
			Balancer:     balancer,
			Topic:        topic,
			BatchTimeout: 100 * time.Millisecond, //生产数据间隔
		}
		fmt.Printf("do once init this.writer\n")
	})
	err := this.writer.WriteMessages(context.Background(), kafka.Message{Key: key, Value: value})
	// fmt.Printf("producer topic(%s) k(%s) v(%s)\n", topic, key, value)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func (c *class) shutdown() {
	if this.writer == nil {
		return
	}
	err := this.writer.Close()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("kafka.Writer close\n")
}

// 数据不保证有序
//    单分区有序
//    多分区无序
// 数据分配到"接收数据量最小"的分区
// 多分区异步写，高吞吐率
func Write(addr, topic string, value []byte) (func(), error) {
	return this.shutdown, this.write(addr, topic, nil, value, &kafka.LeastBytes{})
}

// 数据按key分区
// 同一key数据分到同分区，数据有序
func WriteByKey(addr, topic string, key, value []byte) (func(), error) {
	return this.shutdown, this.write(addr, topic, key, value, &kafka.CRC32Balancer{})
}
