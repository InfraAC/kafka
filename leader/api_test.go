package leader

import (
	"fmt"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestLeaderConn(t *testing.T) {
	var leader *kafka.Conn
	defer leader.Close()
	for i := 0; i < 100000; i++ {
		leader = Conn("127.0.0.1:9092")
		// defer leader.Close()
		fmt.Printf("Leader broker host=%s\n", leader.Broker().Host)
	}
}
