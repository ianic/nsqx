package main

import (
	"fmt"
	"log"
	//"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	//"time"

	"github.com/nsqio/go-nsq"
)

func main() {
	cfg := nsq.NewConfig()
	cfg.MaxInFlight = 16
	// cfg.HeartbeatInterval = 5 * time.Second
	// cfg.MsgTimeout = 2 * time.Second
	// cfg.LookupdPollInterval = 5 * time.Second

	consumers := make([]*nsq.Consumer, 0, 127)

	for i := 0; i < 128; i += 1 {
		topic := fmt.Sprintf("topic-%03d", i)
		channel := fmt.Sprintf("%03d", i)
		consumer, err := nsq.NewConsumer(topic, channel, cfg)
		if err != nil {
			log.Fatal(err)
		}
		consumer.AddHandler(&Handler{})
		//consumer.AddConcurrentHandlers(&Handler{}, 16)

		//err = consumer.ConnectToNSQLookupd("127.0.0.1:4161")
		err = consumer.ConnectToNSQD("127.0.0.1:4150")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("connected %s %s\n", topic, channel)
		consumers = append(consumers, consumer)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	for _, consumer := range consumers {
		consumer.Stop()
	}
	for _, consumer := range consumers {
		<-consumer.StopChan
	}
}

type Handler struct {
	counter int
}

func (th *Handler) HandleMessage(m *nsq.Message) error {
	return nil
}
