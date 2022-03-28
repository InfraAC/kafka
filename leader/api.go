package leader

import (
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/segmentio/kafka-go"
)

type Leader struct {
	conn *kafka.Conn
	once sync.Once
}

var this = &Leader{}

func Conn(addr string) *kafka.Conn {
	this.once.Do(func() {
		conn, err := kafka.Dial("tcp", addr)
		if err != nil {
			panic(err.Error())
		}
		defer conn.Close()
		controller, err := conn.Controller()
		if err != nil {
			panic(err.Error())
		}
		var leader *kafka.Conn
		fmt.Printf("Leader=%s\n", controller.Host)
		leader, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
		if err != nil {
			panic(err.Error())
		}
		this.conn = leader
	})
	return this.conn
}
