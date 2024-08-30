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
	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("%d %d", i, time.Now().Unix())

		// if producer.Publish("topic", []byte(msg)); err != nil {
		// 	log.Fatal(err)
		// }

		if producer.DeferredPublish("topic", time.Second*time.Duration(i), []byte(msg)); err != nil {
			log.Fatal(err)
		}
	}

	// sigChan := make(chan os.Signal, 1)
	// signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	// <-sigChan

	producer.Stop()
}
