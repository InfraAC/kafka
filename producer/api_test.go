package producer

import (
	"fmt"
	"sync"
	"testing"

	"github.com/sharkgulf/kafka/topic/foobar"
)

// 检查kafka.Writer是否tcp链接复用
//     while true;do echo $(date --rfc-3339=seconds);netstat -apn|grep ":9092";sleep 1s;done
func TestP(t *testing.T) {
	for i := 0; i < 100000; i++ {
		// to produce messages
		topic := foobar.Topic
		value := []byte(fmt.Sprintf("%d", i))
		write := New(Option{"127.0.0.1:9092"})
		err := write.Write(topic, value)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("producer topic(%s) k(%v) v(%s)\n", topic, nil, value)
	}
}

//测试写多分区
func TestWriteMultiplePartition(t *testing.T) {
	var goroutine int = 3
	var wg sync.WaitGroup
	wg.Add(goroutine)
	for i := 0; i < goroutine; i++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				// to produce messages
				topic := foobar.Topic
				value := []byte(fmt.Sprintf("%d", i))
				write := New(Option{"127.0.0.1:9092"})
				err := write.Write(topic, value)
				if err != nil {
					fmt.Println(err)
					return
				}
				fmt.Printf("producer topic(%s) k(%v) v(%s)\n", topic, nil, value)
			}
		}()
	}
	wg.Wait()
}
