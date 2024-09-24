package main

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"log"
	"math/rand/v2"
	"sync"
	//"time"
	// "os"
	// "os/signal"
	// "syscall"
)

func main0() {
	// cfg := nsq.NewConfig()

	// producer, err := nsq.NewProducer("127.0.0.1:4150", cfg)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// if err = producer.Ping(); err != nil {
	// 	log.Fatal(err)
	// }

	var wg sync.WaitGroup
	for i := 0; i < 128; i++ {
		wg.Add(1)
		go func(j int) {
			topic := fmt.Sprintf("topic-%03d", j)
			pub(topic)
			wg.Done()
		}(i)
	}
	wg.Wait()

	// sigChan := make(chan os.Signal, 1)
	// signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	// <-sigChan

}

func pub(topic string) {
	cfg := nsq.NewConfig()
	producer, err := nsq.NewProducer("127.0.0.1:4150", cfg)
	if err != nil {
		log.Fatal(err)
	}

	for j := 0; j < 1024*1024; j++ {
		size := rand.IntN(1024 * 16)
		batchSize := rand.IntN(256)

		msg := make([]byte, size)
		// for i := range msg {
		// 	msg[i] = byte(i % 256)
		// }
		batch := make([][]byte, batchSize)
		for i := range batch {
			batch[i] = msg
		}

		if producer.MultiPublish(topic, batch); err != nil {
			log.Fatal(err)
		}
	}
	producer.Stop()
}

func main() {
	cfg := nsq.NewConfig()

	producer, err := nsq.NewProducer("127.0.0.1:4150", cfg)
	if err != nil {
		log.Fatal(err)
	}

	if err = producer.Ping(); err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 1; i++ {
		//msg := fmt.Sprintf("%d %d", i, time.Now().Unix())
		msg := make([]byte, 0)

		if producer.Publish("topic-001", []byte(msg)); err != nil {
			log.Fatal(err)
		}

		// if producer.DeferredPublish("topic", time.Second*time.Duration(i), []byte(msg)); err != nil {
		// 	log.Fatal(err)
		// }
		//time.Sleep(10 * time.Second)
	}

	// sigChan := make(chan os.Signal, 1)
	// signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	// <-sigChan

	producer.Stop()
}
