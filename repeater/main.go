package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/jhleebre/go/kafka"
)

var (
	consumerBrokers  = "localhost:9092"
	consumerTopic    = "consumeTopic"
	consumerGroup    = "consumeGroup"
	consumerAssignor = "range"
	consumerVersion  = "2.4.0"
	consumerOldest   = false
	producerBrokers  = "localhost:9092"
	producerTopic    = "produceTopic"
)

type consumerGroupHandler struct {
	Producer *kafka.Producer
	Ready    chan bool
}

func (c *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	close(c.Ready)
	return nil
}

func (c *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		//fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		c.Producer.Producer.Input() <- &sarama.ProducerMessage{
			Topic: c.Producer.Topic,
			Key:   nil,
			Value: sarama.StringEncoder(msg.Value),
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

func main() {
	config := kafka.NewConfig()

	// xxx: replace follows to parse config file in json format
	config.ConsumerGroup.Brokers = strings.Split(consumerBrokers, ",")
	config.ConsumerGroup.Topic = consumerTopic
	config.ConsumerGroup.Group = consumerGroup
	config.ConsumerGroup.Assignor = consumerAssignor
	config.ConsumerGroup.Version = consumerVersion
	config.ConsumerGroup.Oldest = consumerOldest
	config.Producer.Brokers = strings.Split(producerBrokers, ",")
	config.Producer.Topic = producerTopic

	if err := config.Validate(); err != nil {
		panic(fmt.Sprintln("invalid configuration:", err))
	}

	consumerGroup, err := kafka.NewConsumerGroup(config)
	if err != nil {
		panic(fmt.Sprintln("failed to create consumer group:", err))
	}

	producer, err := kafka.NewProducer(config)
	if err != nil {
		consumerGroup.Close()
		panic(fmt.Sprintln("failed to create producer:", err))
	}

	defer func() {
		consumerGroup.Close()
		producer.Close()
	}()

	go func() {
		for err := range consumerGroup.ConsumerGroup.Errors() {
			fmt.Println("consumer group error:", err)
		}
	}()

	go func() {
		for err := range producer.Producer.Errors() {
			fmt.Println("producer error:", err)
		}
	}()

	go func() {
		printTicker := time.NewTicker(time.Second * 5)
		for {
			select {
			case <-printTicker.C:
				fmt.Println(producer.SaramaConfig.MetricRegistry.GetAll())
			}
		}
	}()

	// iterate over consumer sessions.
	ctx, cancel := context.WithCancel(context.Background())
	handler := consumerGroupHandler{
		Producer: producer,
		Ready:    make(chan bool),
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()

		for {
			err := consumerGroup.ConsumerGroup.Consume(ctx, []string{consumerGroup.Topic}, &handler)
			if err != nil {
				fmt.Println("consumer group error:", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	<-handler.Ready
	fmt.Println("Repeater up and running ...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		fmt.Println("terminating: context cancelled")
	case <-sigterm:
		fmt.Println("terminating: via signal")
	}
	cancel()
	wg.Wait()
}
