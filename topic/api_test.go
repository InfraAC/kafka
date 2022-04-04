package topic

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sharkgulf/kafka/client"
	"github.com/sharkgulf/kafka/controller"
)

var (
	Topic             string = "footbar"
	Partitions        int    = 3
	ReplicationFactor int    = 1
)

func TestCreate(t *testing.T) {
	controller := controller.Conn("127.0.0.1:9092")
	defer controller.Close()
	entry := New(Option{Conn: controller})
	err := entry.Create(Topic, Partitions, ReplicationFactor)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("Topic(%v) create success\n", Topic)
}

func TestQueryList(t *testing.T) {
	controller := controller.Conn("127.0.0.1:9092")
	defer controller.Close()
	entry := New(Option{Conn: controller})
	m := entry.QueryList()
	for k := range m {
		fmt.Println(k)
	}
}

func TestQueryInfo(t *testing.T) {
	controller := controller.Conn("127.0.0.1:9092")
	defer controller.Close()
	entry := New(Option{Conn: controller})
	topic := Topic
	r := entry.Query(topic)
	if r == nil {
		fmt.Printf("Topic(%s) not exist\n", topic)
		return
	}
	fmt.Printf("Topic(%s) exist,detail=%v\n", topic, r)
}

func TestDelete(t *testing.T) {
	controller := controller.Conn("127.0.0.1:9092")
	defer controller.Close()
	entry := New(Option{Conn: controller})
	topics := []string{Topic}
	for _, v := range topics {
		err := entry.Delete(v)
		if err != nil {
			fmt.Printf("Topic(%s) delete,err=%s", v, err)
			return
		}
		fmt.Printf("Topic(%s) delete\n", v)
	}
}

//低级api
func TestLowApiTopicCreate(t *testing.T) {
	config := []kafka.ConfigEntry{{
		ConfigName:  "retention.ms",
		ConfigValue: "3600000",
	}}
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
	res, err := client.CreateTopics(context.Background(), &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{
			{
				Topic:             Topic,
				NumPartitions:     Partitions,
				ReplicationFactor: ReplicationFactor,
				ConfigEntries:     config,
			},
			// {
			// 	Topic:             topic2,
			// 	NumPartitions:     2,
			// 	ReplicationFactor: 1,
			// 	ConfigEntries:     config,
			// },
			// {
			// 	Topic:             topic3,
			// 	NumPartitions:     1,
			// 	ReplicationFactor: 1,
			// 	ConfigEntries:     config,
			// },
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for topic, error := range res.Errors {
		if error != nil {
			t.Errorf("%s => %s", topic, error)
		}
		fmt.Printf("Topic(%s) create success\n", topic)
	}
}
