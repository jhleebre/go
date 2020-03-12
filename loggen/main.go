package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
)

var configFile = "./config/loggen-config.json"

// LogMessage defines the log message format to produce.
type LogMessage struct {
	Fields struct {
		Value string `json:"value"`
	} `json:"fields"`
	Name string `json:"name"`
	Tags struct {
		GetTime   uint64 `json:"get_time"`
		Host      string `json:"host"`
		LogTypeID string `json:"log_type_id"`
		Path      string `json:"path"`
		SendTime  uint64 `json:"send_time"`
		TcoreID   uint32 `json:"tcore_id"`
	} `json:"tags"`
	Timestamp uint64 `json:"timestamp"`
}

// Producer is a wrapper of the Sarama's producer
type Producer struct {
	Brokers       []string
	Topic         string
	RequiredAcks  string
	Compression   string
	NumLogSamples int
	MsecPeriod    int
	Duplication   int
	SaramaConfig  *sarama.Config
	Producer      sarama.AsyncProducer
}

// ProducerConfig is the namespace for configuration related to producing messages.
type ProducerConfig struct {
	Brokers       []string `json:"brokers"`
	Topic         string   `json:"topic"`
	RequiredAcks  string   `json:"required_acks"`
	Compression   string   `json:"compression"`
	NumLogSamples int      `json:"num_log_samples"`
	MsecPeriod    int      `json:"msec_period"`
	Duplication   int      `json:"duplication"`
}

// NewProducer creates a new producer for the given configuration.
func NewProducer(config *ProducerConfig) (*Producer, error) {
	p := &Producer{}

	p.Brokers = config.Brokers
	p.Topic = config.Topic
	p.RequiredAcks = config.RequiredAcks
	p.Compression = config.Compression
	p.NumLogSamples = config.NumLogSamples
	p.MsecPeriod = config.MsecPeriod
	p.Duplication = config.Duplication
	p.SaramaConfig = sarama.NewConfig()

	switch p.RequiredAcks {
	case "no_response":
		p.SaramaConfig.Producer.RequiredAcks = sarama.NoResponse
	case "wait_for_local":
		p.SaramaConfig.Producer.RequiredAcks = sarama.WaitForLocal
	case "wait_for_all":
		p.SaramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	default:
		return nil, errors.New(fmt.Sprintln("invalid required ACKs:", p.RequiredAcks))
	}

	switch p.Compression {
	case "none":
		p.SaramaConfig.Producer.Compression = sarama.CompressionNone
	case "gzip":
		p.SaramaConfig.Producer.Compression = sarama.CompressionGZIP
	case "snappy":
		p.SaramaConfig.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		p.SaramaConfig.Producer.Compression = sarama.CompressionLZ4
	case "zstd":
		p.SaramaConfig.Producer.Compression = sarama.CompressionZSTD
	default:
		return nil, errors.New(fmt.Sprintln("invalid compression:", p.Compression))
	}

	ap, err := sarama.NewAsyncProducer(p.Brokers, p.SaramaConfig)
	if err != nil {
		return nil, err
	}

	p.Producer = ap

	return p, nil
}

// NewProducerConfig returns a new producer configuration instance with sane defaults.
func NewProducerConfig() *ProducerConfig {
	p := &ProducerConfig{}

	p.Brokers = []string{""}
	p.Topic = ""
	p.RequiredAcks = "no_response"
	p.Compression = "none"

	return p
}

// LoadProducerConfig reads given configuration file in JSON format and set the configuration.
func (p *ProducerConfig) LoadProducerConfig(filename string) error {
	jsonFile, err := os.Open(filename)
	if err != nil {
		return err
	}

	defer jsonFile.Close()

	byteValue, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		return err
	}

	err = json.Unmarshal(byteValue, p)
	if err != nil {
		return err
	}

	return nil
}

// Close stops the Producer and detaches any running sessions.
func (p *Producer) Close() error {
	if err := p.Producer.Close(); err != nil {
		fmt.Println("failed to shut down producer cleanly:", err)
	}
	return nil
}

// Produce the byte value.
func (p *Producer) Produce(byteValue []byte) {
	p.Producer.Input() <- &sarama.ProducerMessage{
		Topic: p.Topic,
		Key:   nil,
		Value: sarama.ByteEncoder(byteValue),
	}
}

func main() {
	p := NewProducerConfig()
	if err := p.LoadProducerConfig(configFile); err != nil {
		panic(fmt.Sprintln("failed to load configuration:", err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		launchProducer(ctx, p)
		wg.Done()
	}()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		fmt.Println("terminating: context cancelled")
	case <-sigterm:
		fmt.Println("terminating: via siganl")
	}

	cancel()
	wg.Wait()
}

func launchProducer(ctx context.Context, p *ProducerConfig) {
	producer, err := NewProducer(p)
	if err != nil {
		fmt.Println("failed to create producer:", err)
		return
	}

	defer producer.Close()

	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)

	ticker := time.NewTicker(time.Millisecond * time.Duration(producer.MsecPeriod))

	for {
		select {
		case <-ticker.C:
			idx := r.Intn(producer.NumLogSamples)

			msg := &LogMessage{}
			msg.Fields.Value = fmt.Sprintf("sample log message %d", idx)
			msg.Name = "tail"
			msg.Tags.GetTime = uint64(time.Now().UnixNano())
			msg.Tags.Host = "sample_log_msg.com"
			msg.Tags.LogTypeID = "syslog"
			msg.Tags.Path = "/var/log/messages"
			msg.Tags.SendTime = msg.Tags.GetTime
			msg.Tags.TcoreID = uint32(idx + 100)
			msg.Timestamp = msg.Tags.GetTime

			for i := 0; i < producer.Duplication; i++ {
				if i > 0 {
					msg.Tags.LogTypeID = fmt.Sprint(i)
				}
				data, err := json.Marshal(*msg)
				if err != nil {
					fmt.Println("error in JSON marshal:", err)
					continue
				}
				fmt.Println(string(data))
				producer.Produce(data)
			}
		case <-ctx.Done():
			return
		}
	}
}
