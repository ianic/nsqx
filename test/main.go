package main

import (
	"github.com/nsqio/go-nsq"
	"log"
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

	print("subscribed")

}

type TailHandler struct{}

func (th *TailHandler) HandleMessage(m *nsq.Message) error {
	return nil
}
