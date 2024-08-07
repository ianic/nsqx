package main

import (
	"github.com/nsqio/go-nsq"
	"log"

	"os"
	"os/signal"
	"syscall"
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
	if producer.Publish("topic", []byte("iso medo u ducan nije reko dobar dan")); err != nil {
		log.Fatal(err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	producer.Stop()
}
