package controller

import (
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/segmentio/kafka-go"
)

type Controller struct {
	conn *kafka.Conn
	once sync.Once
}

var this = &Controller{}

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
		var controllerConn *kafka.Conn
		fmt.Printf("controller=%#+v\n", controller)
		controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
		if err != nil {
			panic(err.Error())
		}
		this.conn = controllerConn
	})
	return this.conn
}
