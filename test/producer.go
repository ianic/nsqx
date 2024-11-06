package main

import (
	"fmt"
	"github.com/nsqio/go-nsq"
	"log"
	"math/rand/v2"
	"sync"
	"time"
	// "os"
	// "os/signal"
	// "syscall"
)

func main() {
	cfg := nsq.NewConfig()
	cfg.HeartbeatInterval = 1 * time.Second
	cfg.LookupdPollInterval = 1 * time.Second // reconnection
	cfg.MaxBackoffDuration = 1 * time.Second
	cfg.ReadTimeout = 5 * time.Second
	cfg.WriteTimeout = 5 * time.Second

	producer, err := nsq.NewProducer("127.0.0.1:4150", cfg)
	if err != nil {
		log.Fatal(err)
	}
	producer.SetLoggerLevel(nsq.LogLevelError)

	var wg sync.WaitGroup
	for i := 0; i < 128; i++ {
		wg.Add(1)
		go func(j int) {
			topic := fmt.Sprintf("topic-%03d", j)
			pub2(producer, topic)
			//print("done", j)
			wg.Done()
		}(i)
	}
	wg.Wait()
	producer.Stop()

	// cfg := nsq.NewConfig()

	// producer, err := nsq.NewProducer("127.0.0.1:4150", cfg)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// if err = producer.Ping(); err != nil {
	// 	log.Fatal(err)
	// }

	// var wg sync.WaitGroup
	// for i := 1; i < 16; i++ {
	// 	wg.Add(1)
	// 	go func(j int) {
	// 		topic := fmt.Sprintf("topic-%03d", j)
	// 		pub(topic)
	// 		wg.Done()
	// 	}(i)
	// }
	// wg.Wait()

	// sigChan := make(chan os.Signal, 1)
	// signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	// <-sigChan

}

func pub2(producer *nsq.Producer, topic string) {
	n := 0
	for j := 0; j < 128; j++ {
		size := rand.IntN(1024*16) + 1
		batchSize := rand.IntN(256) + 1

		batch := make([][]byte, batchSize)
		for k := range batch {
			msg := make([]byte, size)
			for l := range msg {
				msg[l] = byte(n % 256)
				n += 1
			}
			batch[k] = msg
		}

		if err := producer.MultiPublishAsync(topic, batch, nil); err != nil {
			log.Fatal(err)
		}
	}
	//print(topic, " done\n")
}

func mpub(topic string) {
	cfg := nsq.NewConfig()
	cfg.HeartbeatInterval = 1 * time.Second
	cfg.LookupdPollInterval = 1 * time.Second // reconnection
	cfg.MaxBackoffDuration = 1 * time.Second
	cfg.ReadTimeout = 5 * time.Second
	cfg.WriteTimeout = 5 * time.Second

	producer, err := nsq.NewProducer("127.0.0.1:4150", cfg)
	if err != nil {
		log.Fatal(err)
	}
	producer.SetLoggerLevel(nsq.LogLevelError)

	for j := 0; j < 128; j++ {
		size := rand.IntN(1024 * 16)
		batchSize := 256 //rand.IntN(256)

		msg := make([]byte, size)
		// for i := range msg {
		// 	msg[i] = byte(i % 256)
		// }
		batch := make([][]byte, batchSize)
		for i := range batch {
			batch[i] = msg
		}

		if err = producer.MultiPublish(topic, batch); err != nil {
			log.Fatal(err)
		}
	}
	print(topic, " done\n")
	producer.Stop()
}

func pub(topic string) {
	cfg := nsq.NewConfig()
	cfg.HeartbeatInterval = 1 * time.Second
	cfg.LookupdPollInterval = 1 * time.Second // reconnection
	cfg.MaxBackoffDuration = 1 * time.Second
	cfg.ReadTimeout = 5 * time.Second
	cfg.WriteTimeout = 5 * time.Second

	producer, err := nsq.NewProducer("127.0.0.1:4150", cfg)
	if err != nil {
		log.Fatal(err)
	}
	producer.SetLoggerLevel(nsq.LogLevelError)
	// if err := producer.Ping(); err != nil {
	// 	log.Fatal(err)
	// }

	for j := 0; j < 16*1024; j++ {
		size := rand.IntN(1024 * 16)

		msg := make([]byte, size)
		for i := range msg {
			msg[i] = byte(i % 256)
		}

		for {
			if err = producer.Publish(topic, msg); err != nil {
				// print("publish error ", err.Error(), "\n")
				//log.Fatal(err)
				//panic(err)
				continue
			}
			break
		}
		// print(topic, " ", j, "\n")
	}
	print(topic, " done\n")
	producer.Stop()
}

func main0() {
	cfg := nsq.NewConfig()

	producer, err := nsq.NewProducer("127.0.0.1:4150", cfg)
	if err != nil {
		log.Fatal(err)
	}

	if err = producer.Ping(); err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 1; i++ {
		msg := fmt.Sprintf("%d %d", i, time.Now().Unix())
		//msg := make([]byte, 0)

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
