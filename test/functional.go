package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nsqio/go-nsq"
)

var (
	wait = flag.Bool("wait", false, "wait for interrupt after finish")
)

func main() {
	flag.Parse()
	const file_name = "/home/ianic/Code/tls.zig/example/cert/pg2600.txt"

	cfg := nsq.NewConfig()
	cfg.HeartbeatInterval = 1 * time.Second
	cfg.LookupdPollInterval = 1 * time.Second // reconnection
	cfg.MaxBackoffDuration = 1 * time.Second
	cfg.ReadTimeout = 5 * time.Second
	cfg.WriteTimeout = 5 * time.Second
	cfg.MsgTimeout = 1 * time.Second

	producer, err := nsq.NewProducer("127.0.0.1:4150", cfg)
	if err != nil {
		log.Fatal(err)
	}
	producer.SetLoggerLevel(nsq.LogLevelError)

	timestamp := time.Now().Format("2006-01-02-15-04-05")
	lines_topic := fmt.Sprintf("book-lines-%s", timestamp)

	content, err := os.ReadFile(file_name)
	if err != nil {
		log.Fatal(err)
	}
	//"\r\nCHAPTER "
	lines := strings.SplitAfter(string(content), "\r\n")
	messages := len(lines)
	consumers := make([]*nsq.Consumer, 0)

	var wg sync.WaitGroup

	wg.Add(1)
	var handler1 = MsgsHandler{
		wg:       &wg,
		expected: lines,
		attempts: make([]byte, len(lines)),
	}
	consumer, err := startConsumer(lines_topic, "channel1", cfg, &handler1)
	if err != nil {
		log.Fatal(err)
	}
	consumers = append(consumers, consumer)

	wg.Add(1)
	var handler2 = MsgsHandler{
		wg:       &wg,
		expected: lines,
		attempts: make([]byte, len(lines)),
	}
	consumer, err = startConsumer(lines_topic, "channel2", cfg, &handler2)
	if err != nil {
		log.Fatal(err)
	}
	consumers = append(consumers, consumer)
	consumer, err = startConsumer(lines_topic, "channel2", cfg, &handler2)
	if err != nil {
		log.Fatal(err)
	}
	consumers = append(consumers, consumer)

	wg.Add(1)
	var handler3 = MsgsHandler{
		wg:       &wg,
		expected: lines,
		attempts: make([]byte, len(lines)),
	}
	consumer, err = startConsumer(lines_topic, "channel3", cfg, &handler3)
	if err != nil {
		log.Fatal(err)
	}
	consumers = append(consumers, consumer)
	consumer, err = startConsumer(lines_topic, "channel3", cfg, &handler3)
	if err != nil {
		log.Fatal(err)
	}
	consumers = append(consumers, consumer)
	consumer, err = startConsumer(lines_topic, "channel3", cfg, &handler3)
	if err != nil {
		log.Fatal(err)
	}
	consumers = append(consumers, consumer)

	body := make([][]byte, len(lines))
	for i, line := range lines {
		body[i] = []byte(line)
	}
	if err := producer.MultiPublish(lines_topic, body); err != nil {
		log.Fatal(err)
	}
	// for _, line := range lines {
	// 	if err := producer.Publish(lines_topic, []byte(line)); err != nil {
	// 		log.Fatal(err)
	// 	}
	// }

	wg.Wait()

	handler1.expect(messages)
	handler2.expect(messages)
	handler3.expect(messages)

	stats, err := getStats(lines_topic)
	if err != nil {
		log.Fatal(err)
	}
	expectMessages(stats, "channel3", 3, messages)
	expectMessages(stats, "channel2", 2, messages)
	expectMessages(stats, "channel1", 1, messages)

	if *wait {
		fmt.Printf("\ntopic: %s\n", lines_topic)
		fmt.Printf("waiting for ctr-c\n")
		waitInterrupt()
	}

	for _, consumer = range consumers {
		consumer.Stop()
		<-consumer.StopChan
	}

	if err := deleteTopic(lines_topic); err != nil {
		log.Fatal(err)
	}
}

func waitInterrupt() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
}

func expectMessages(stats *Stats, channel string, clients int, messages int) {
	for _, c := range stats.Topics[0].Channels {
		if c.ChannelName == channel {
			fmt.Printf("%s messages: %d requeued: %d timed out: %d\n", c.ChannelName, c.MessageCount, c.RequeueCount, c.TimeoutCount)
			if c.MessageCount != messages {
				log.Fatalf("channel %s expected message count %d actual %d", channel, messages, c.MessageCount)
			}
			var fin_total int
			//fin_expected := int(messages / clients)
			for i, cli := range c.Clients {
				fmt.Printf("  client %d finished: %d requeued: %d\n", i, cli.FinishCount, cli.RequeueCount)
				// if fin < fin_expected-2 || fin > fin_expected+2 {
				// 	log.Fatalf("channel %s client %d expected fin %d actual %d", channel, i, fin_expected, fin)
				// }
				fin_total += cli.FinishCount
			}
			if fin_total != messages {
				log.Fatalf("channel %s expected fin count %d actual %d", channel, messages, fin_total)
			}
			return
		}
	}
	log.Fatalf("channel %s not found", channel)
}

func getStats(topic string) (*Stats, error) {
	url := fmt.Sprintf("http://localhost:4151/stats?format=json&include_clients=1&topic=%s", topic)
	var cli = http.Client{Timeout: 10 * time.Second}
	r, err := cli.Get(url)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()

	var stats Stats
	if err := json.NewDecoder(r.Body).Decode(&stats); err != nil {
		return nil, err
	}
	return &stats, nil
}

func deleteTopic(topic string) error {
	url := fmt.Sprintf("http://localhost:4151/topic/delete?topic=%s", topic)
	_, err := http.Get(url)
	if err != nil {
		return err
	}
	// try to delete in lookupd's also
	for _, lookupd_port := range []int{4161, 4163} {
		url = fmt.Sprintf("http://localhost:%d/topic/delete?topic=%s", lookupd_port, topic)
		_, err = http.Post(url, "", nil)
		if err != nil {
			log.Printf("lookupd topic delete error %s, url: %s", err, url)
		}
	}
	return nil
}

type Stats struct {
	Topics []Topic
}
type Topic struct {
	TopicName    string `json:"topic_name"`
	MessageCount int    `json:"message_count"`
	MessageBytes int    `json:"message_bytes"`
	Channels     []Channel
}
type Channel struct {
	ChannelName   string `json:"channel_name"`
	Depth         int    `json:"depth"`
	BackendDepth  int    `json:"backend_depth"`
	InFlightCount int    `json:"in_flight_count"`
	DeferredCount int    `json:"deferred_count"`
	MessageCount  int    `json:"message_count"`
	RequeueCount  int    `json:"requeue_count"`
	TimeoutCount  int    `json:"timeout_count"`
	ClientCount   int    `json:"client_count"`
	Paused        bool   `json:"paused"`
	Clients       []Client
}
type Client struct {
	InFlightCount int `json:"in_flight_count"`
	MessageCount  int `json:"message_count"`
	FinishCount   int `json:"finish_count"`
	RequeueCount  int `json:"requeue_count"`
}
type MsgsHandler struct {
	wg       *sync.WaitGroup
	expected []string
	attempts []byte
	ok       int
	fail     int
	sync.Mutex
}

const requeue_every = 1024
const timeout_idx = 512

func (h *MsgsHandler) HandleMessage(m *nsq.Message) error {
	h.Lock()

	id := [16]byte(m.ID)
	sequence := binary.BigEndian.Uint64(id[8:16])
	idx := sequence - 1

	h.attempts[idx] = h.attempts[idx] + 1

	if m.Attempts == 1 && idx == timeout_idx {
		h.Unlock()
		time.Sleep(time.Second * 2)
		return nil
	}
	defer h.Unlock()

	// if m.Attempts == 1 && idx == 513 {
	// 	m.Requeue(time.Second)
	// 	return nil
	// }

	if m.Attempts == 1 && idx%requeue_every == 0 {
		m.Requeue(0)
		return nil
	}

	if h.expected[idx] == string(m.Body) {
		h.ok += 1
	} else {
		h.fail += 1
	}
	if (h.ok + h.fail) == len(h.expected) {
		h.wg.Done()
	}
	return nil
}

func (h *MsgsHandler) expect(messages int) {
	if h.fail != 0 || h.ok != messages {
		log.Fatalf("handler messages %d ok %d fail %d", messages, h.ok, h.fail)
	}
	for idx, a := range h.attempts {
		if idx%requeue_every == 0 || idx == timeout_idx {
			if a != 2 {
				log.Fatalf("expected 2 attempts %d got %d", idx, a)
			}
		} else if a != 1 {
			log.Fatalf("expected 1 attempts %d got %d", idx, a)
		}
	}
}

func startConsumer(topic string, channel string, cfg *nsq.Config, handler *MsgsHandler) (*nsq.Consumer, error) {
	consumer, err := nsq.NewConsumer(topic, channel, cfg)
	if err != nil {
		return nil, err
	}
	consumer.SetLoggerLevel(nsq.LogLevelError)
	//consumer.AddHandler(handler)
	consumer.AddConcurrentHandlers(handler, 16)
	consumer.ChangeMaxInFlight(16)
	consumer.SetLoggerForLevel(nil, nsq.LogLevelError)

	if err := consumer.ConnectToNSQD("127.0.0.1:4150"); err != nil {
		return nil, err
	}
	return consumer, nil
}
