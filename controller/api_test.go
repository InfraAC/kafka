package controller

import (
	"fmt"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestLeaderConn(t *testing.T) {
	var controller *kafka.Conn
	defer controller.Close()
	for i := 0; i < 100000; i++ {
		controller = Conn("127.0.0.1:9092")
		// defer controller.Close()
		fmt.Printf("controller broker host=%s\n", controller.Broker().Host)
	}
}
