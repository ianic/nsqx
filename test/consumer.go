package main

import (
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nsqio/go-nsq"
)

func main() {
	cfg := nsq.NewConfig()
	cfg.MaxInFlight = 16

	consumer, err := nsq.NewConsumer("topic", "channel", cfg)
	if err != nil {
		log.Fatal(err)
	}
	//consumer.AddHandler(&Handler{})
	consumer.AddConcurrentHandlers(&Handler{}, 16)

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

type Handler struct {
	counter int
}

func (th *Handler) HandleMessage(m *nsq.Message) error {
	sleep := rand.IntN(30)
	time.Sleep(time.Duration(sleep) * time.Second)
	fmt.Printf("%s attempts: %d sleep: %d\n", m.Body, m.Attempts, sleep)
	return nil
}

func (th *Handler) HandleMessage2(m *nsq.Message) error {
	m.DisableAutoResponse()
	th.counter += 1
	fin := th.counter%2 == 0

	fmt.Printf("%s %d %v\n", m.Body, th.counter, fin)
	if fin {
		m.Finish()
	} else {
		m.Requeue(0)
	}

	//fmt.Printf(".")
	return nil
}
