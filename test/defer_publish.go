package main

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"log"
	"time"
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
		msg := fmt.Sprintf("%03d %s", i, time.Now().Format(time.RFC1123))

		if producer.DeferredPublish("topic2", time.Second*time.Duration(i), []byte(msg)); err != nil {
			log.Fatal(err)
		}
		//time.Sleep(10 * time.Second)
	}

	// sigChan := make(chan os.Signal, 1)
	// signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	// <-sigChan

	producer.Stop()
}
