package consumer

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	Topic             string = "footbar"
	Partitions        int    = 3
	ReplicationFactor int    = 1
)

func TestCG(t *testing.T) {
	// make a new reader that consumes from topic-A
	topic := Topic
	group := fmt.Sprintf("consumer-group-%s", topic)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"127.0.0.1:9092"},
		GroupID:  group,
		Topic:    topic,
		MinBytes: 1,    // 10KB
		MaxBytes: 10e6, // 10MB
		// ReadBackoffMin:  1000 * time.Millisecond, //消费数据间隔
		ReadLagInterval: 100 * time.Millisecond, //消费数据间隔
		CommitInterval:  time.Second,            // flushes commits to Kafka every second
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

func TestRead(t *testing.T) {
	addr := "127.0.0.1:9092"
	topic := Topic
	group := fmt.Sprintf("consumer-group-%s", topic)
	entry := New(Option{
		Addr:  addr,
		Group: group,
		Topic: topic,
	})
	entry.OnMessage = OnMessage(func(message []byte) bool {
		fmt.Printf("onMessage=%s", message)
		return true
	})
}
