package topic

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"gogs.sharkgulf.cn/sg/bs/qkafka/client"
	"gogs.sharkgulf.cn/sg/bs/qkafka/leader"
)

func TestCreate(t *testing.T) {
	leader := leader.Conn("127.0.0.1:9092")
	defer leader.Close()
	entry := New(Option{Conn: leader})
	topic := "foobar"
	err := entry.Create(topic, 1, 1)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("Topic(%s) create success\n", topic)
}

func TestQueryList(t *testing.T) {
	leader := leader.Conn("127.0.0.1:9092")
	defer leader.Close()
	entry := New(Option{Conn: leader})
	m := entry.QueryList()
	for k := range m {
		fmt.Println(k)
	}
}

func TestQuery(t *testing.T) {
	leader := leader.Conn("127.0.0.1:9092")
	defer leader.Close()
	entry := New(Option{Conn: leader})
	topic := "foobar"
	r := entry.Query(topic)
	if r == nil {
		fmt.Printf("Topic(%s) not exist\n", topic)
		return
	}
	fmt.Printf("Topic(%s) exist,detail=%v\n", topic, r)
}

func TestDelete(t *testing.T) {
	leader := leader.Conn("127.0.0.1:9092")
	defer leader.Close()
	entry := New(Option{Conn: leader})
	topics := []string{
		"foobar",
	}
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
	const (
		topic1 = "client-topic-1"
		topic2 = "client-topic-2"
		topic3 = "client-topic-3"
	)
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
				Topic:             topic1,
				NumPartitions:     -1,
				ReplicationFactor: -1,
				ReplicaAssignments: []kafka.ReplicaAssignment{
					{
						Partition: 0,
						Replicas:  []int{1},
					},
					{
						Partition: 1,
						Replicas:  []int{1},
					},
					{
						Partition: 2,
						Replicas:  []int{1},
					},
				},
				ConfigEntries: config,
			},
			{
				Topic:             topic2,
				NumPartitions:     2,
				ReplicationFactor: 1,
				ConfigEntries:     config,
			},
			{
				Topic:             topic3,
				NumPartitions:     1,
				ReplicationFactor: 1,
				ConfigEntries:     config,
			},
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
