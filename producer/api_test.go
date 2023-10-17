package producer

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/infraac/kafka/client"
)

var (
	Topic             string = "footbar"
	Partitions        int    = 3
	ReplicationFactor int    = 1
)

// 检查kafka.Writer是否tcp链接复用
//     while true;do echo $(date --rfc-3339=seconds);netstat -apn|grep ":9092";sleep 1s;done
func TestP(t *testing.T) {
	for i := 0; i < 10; i++ {
		// to produce messages
		topic := Topic
		value := []byte(fmt.Sprintf("%d", i))
		shutdown, err := Write("127.0.0.1:9092", topic, value)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer shutdown()
		fmt.Printf("producer topic(%s) k(%v) v(%s)\n", topic, nil, value)
	}
}

// // NewUUID func
// func NewUUID() string {
// 	uuid := make([]byte, 128)
// 	n, err := io.ReadFull(rand.Reader, uuid)
// 	if n != len(uuid) || err != nil {
// 		fmt.Println(err)
// 		return ""
// 	}
// 	// variant bits; see section 4.1.1
// 	uuid[8] = uuid[8]&^0xc0 | 0x80
// 	// version 4 (pseudo-random); see section 4.1.3
// 	uuid[6] = uuid[6]&^0xf0 | 0x40
// 	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:])
// }

//测试写多分区
func TestWriteMultiplePartition(t *testing.T) {
	var goroutine int = 3
	var wg sync.WaitGroup
	wg.Add(goroutine)
	for n := 0; n < goroutine; n++ {
		go func(n int) {
			for i := 0; i < 100; i++ {
				// to produce messages
				topic := Topic
				value := []byte(fmt.Sprintf("%016x", rand.Int63()))
				_, err := Write("127.0.0.1:9092", topic, value)
				if err != nil {
					fmt.Println(err)
					return
				}
				fmt.Printf("producer n(%d) topic(%s) k(%v) v(%s)\n", n, topic, nil, value)
			}
			defer wg.Done()
		}(n)
	}
	wg.Wait()
}

//测试key分区
func TestWriteByKey(t *testing.T) {
	var goroutine int = 3
	var wg sync.WaitGroup
	wg.Add(goroutine)
	for n := 0; n < goroutine; n++ {
		go func(n int) {
			for i := 0; i < 100; i++ {
				// to produce messages
				topic := Topic
				// key := []byte(fmt.Sprintf("%016x", rand.Int63()))
				key := []byte(fmt.Sprintf("%d", i))
				value := []byte(fmt.Sprintf("%016x", rand.Int63()))
				_, err := WriteByKey("127.0.0.1:9092", topic, key, value)
				if err != nil {
					fmt.Println(err)
					return
				}
				fmt.Printf("producer n(%d) topic(%s) k(%s) v(%s)\n", n, topic, key, value)
			}
			defer wg.Done()
		}(n)
	}
	wg.Wait()
}

//低级api
func TestLowApiWrite(t *testing.T) {
	conns := &client.ConnWaitGroup{
		DialFunc: (&net.Dialer{}).DialContext,
	}
	transport := &kafka.Transport{
		Dial:     conns.Dial,
		Resolver: kafka.NewBrokerResolver(nil),
	}
	client := &kafka.Client{
		Addr:      kafka.TCP("127.0.0.1:9092"),
		Timeout:   5 * time.Second,
		Transport: transport,
	}
	now := time.Now()
	res, err := client.Produce(context.Background(), &kafka.ProduceRequest{
		Topic:        Topic,
		Partition:    0,
		RequiredAcks: -1,
		Records: kafka.NewRecordReader(
			kafka.Record{Time: now, Value: kafka.NewBytes([]byte(`hello-1`))},
			kafka.Record{Time: now, Value: kafka.NewBytes([]byte(`hello-2`))},
			kafka.Record{Time: now, Value: kafka.NewBytes([]byte(`hello-3`))},
		),
	})

	if err != nil {
		t.Fatal(err)
	}

	if res.Error != nil {
		t.Error(res.Error)
	}

	for index, err := range res.RecordErrors {
		t.Errorf("record at index %d produced an error: %v", index, err)
	}
}

func TestApiWrite(t *testing.T) {
	// make a writer that produces to topic-A, using the least-bytes distribution
	w := &kafka.Writer{
		Addr:     kafka.TCP("127.0.0.1:9092"),
		Topic:    Topic,
		Balancer: &kafka.RoundRobin{},
	}

	//1. RoundRobin is Ok
	err := w.WriteMessages(context.Background(),
		kafka.Message{Value: []byte("Hello World!")},
		kafka.Message{Value: []byte("Hello World!")},
		kafka.Message{Value: []byte("Hello World!")},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	for i := 0; i < 3; i++ {
		err := w.WriteMessages(context.Background(),
			kafka.Message{Value: []byte("Hello kafka")},
		)
		if err != nil {
			log.Fatal("failed to write messages:", err)
			continue
		}
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
