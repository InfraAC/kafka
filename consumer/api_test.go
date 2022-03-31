package consumer

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sharkgulf/kafka/topic/foobar"
)

func TestCG(t *testing.T) {
	// make a new reader that consumes from topic-A
	topic := foobar.Topic
	group := fmt.Sprintf("consumer-group-%s", topic)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"127.0.0.1:9092"},
		GroupID:  group,
		Topic:    topic,
		MinBytes: 1,    // 10KB
		MaxBytes: 10e6, // 10MB
		// ReadBackoffMin:  1000 * time.Millisecond, //消费数据间隔
		ReadLagInterval: 100 * time.Millisecond, //消费数据间隔
	})

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("consumer group(%s) topic(%s) partition(%d) offset(%d) k(%s) v(%s)\n", group, m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}
