package main

import (
	"fmt"
	"log"
	"time"

	"github.com/nsqio/go-nsq"
	// "os"
	// "os/signal"
	// "syscall"
)

func main() {
	cfg := nsq.NewConfig()

	producer, err := nsq.NewProducer("127.0.0.1:4150", cfg)
	if err != nil {
		log.Fatal(err)
	}

	if err = producer.Ping(); err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 128; i++ {
		topic := fmt.Sprintf("topic-%03d", i)
		msg := fmt.Sprintf("%d %d", i, time.Now().Unix())

		if producer.Publish(topic, []byte(msg)); err != nil {
			log.Fatal(err)
		}
	}

	producer.Stop()
}
