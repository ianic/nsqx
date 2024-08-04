package main

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg := nsq.NewConfig()

	consumer, err := nsq.NewConsumer("topic", "channel", cfg)
	if err != nil {
		log.Fatal(err)
	}
	consumer.AddHandler(&TailHandler{})

	err = consumer.ConnectToNSQD("127.0.0.1:4150")
	if err != nil {
		log.Fatal(err)
	}

	print("subscribed\n")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	consumer.Stop()
	<-consumer.StopChan
}

type TailHandler struct{}

func (th *TailHandler) HandleMessage(m *nsq.Message) error {
	fmt.Printf("%s", m.Body)
	return nil
}
