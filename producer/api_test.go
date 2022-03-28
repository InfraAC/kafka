package producer

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// 检查kafka.Writer是否tcp链接复用
//     while true;do echo $(date --rfc-3339=seconds);netstat -apn|grep ":9092";sleep 1s;done
func TestP(t *testing.T) {
	for i := 0; i < 100000; i++ {
		// to produce messages
		topic := "foobar"
		// make a writer that produces to topic-A, using the least-bytes distribution
		w := &kafka.Writer{
			Addr:     kafka.TCP("127.0.0.1:9092"),
			Balancer: &kafka.Hash{}, //相同key分配到同一分区
			// Balancer: &kafka.LeastBytes{}, //数据分配到"接收数据量最小"的分区
			Topic:        topic,
			BatchTimeout: 1 * time.Millisecond,
		}
		err := w.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte("1"),
			Value: []byte(fmt.Sprintf("%d", i)),
		})
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}
	}
}
