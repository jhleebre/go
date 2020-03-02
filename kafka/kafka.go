package kafka

import (
	"errors"
	"fmt"

	"github.com/Shopify/sarama"
)

// Config is used to pass multiple configuration options to Collector's constructors.
type Config struct {
	// ConsumerGroup is the namespace for configuration related to consuming messages,
	// used by the ConsumerGroup.
	ConsumerGroup struct {
		Brokers  []string
		Topic    string
		Group    string
		Assignor string
		Version  string
		Oldest   bool
	}
	// Producer is the namespace for configuration related to producing messages,
	// used by the Producer.
	Producer struct {
		Brokers []string
		Topic   string
	}
}

// NewConfig returns a new configuration instance with sane defaults.
func NewConfig() *Config {
	c := &Config{}

	c.ConsumerGroup.Brokers = []string{""}
	c.ConsumerGroup.Topic = ""
	c.ConsumerGroup.Group = ""
	c.ConsumerGroup.Assignor = "range"
	c.ConsumerGroup.Version = "2.4.0"
	c.ConsumerGroup.Oldest = false

	c.Producer.Brokers = []string{""}
	c.Producer.Topic = ""

	return c
}

// Validate checks a Config instance.
func (c *Config) Validate() error {
	switch {
	case len(c.ConsumerGroup.Brokers) == 0:
		return errors.New("no Kafka bootstrap brokers for consumer group")
	case len(c.ConsumerGroup.Topic) == 0:
		return errors.New("no topic for consumer group")
	case len(c.ConsumerGroup.Group) == 0:
		return errors.New("no group for consumer group")
	case len(c.Producer.Brokers) == 0:
		return errors.New("no Kafka bootstrap brokers for producer")
	}
	return nil
}

// ConsumerGroup is a wrapper of the Sarama's consumer group.
type ConsumerGroup struct {
	Brokers       []string
	Topic         string
	Group         string
	Assignor      string
	Version       string
	Oldest        bool
	ConsumerGroup sarama.ConsumerGroup
}

// NewConsumerGroup creates a new consumer group for the given configuration.
func NewConsumerGroup(config *Config) (*ConsumerGroup, error) {
	consumerGroup := &ConsumerGroup{}

	consumerGroup.Brokers = config.ConsumerGroup.Brokers
	consumerGroup.Topic = config.ConsumerGroup.Topic
	consumerGroup.Group = config.ConsumerGroup.Group
	consumerGroup.Assignor = config.ConsumerGroup.Assignor
	consumerGroup.Version = config.ConsumerGroup.Version
	consumerGroup.Oldest = config.ConsumerGroup.Oldest

	saramaConfig := sarama.NewConfig()

	version, err := sarama.ParseKafkaVersion(consumerGroup.Version)
	if err != nil {
		return nil, err
	}
	saramaConfig.Version = version

	switch consumerGroup.Assignor {
	case "sticky":
		saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "roundrobin":
		saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "range":
		saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	default:
		return nil, errors.New(fmt.Sprintln("invalid assignor:", consumerGroup.Assignor))
	}

	if consumerGroup.Oldest {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	g, err := sarama.NewConsumerGroup(consumerGroup.Brokers, consumerGroup.Group, saramaConfig)
	if err != nil {
		return nil, err
	}

	consumerGroup.ConsumerGroup = g

	return consumerGroup, nil
}

// Close stop the ConsumerGroup and detaches any running sessions.
func (c *ConsumerGroup) Close() error {
	if err := c.ConsumerGroup.Close(); err != nil {
		fmt.Println("failed to shut down consumer group cleanly.", err)
	}
	return nil
}

// Producer is a wrapper of the Sarama's producer.
type Producer struct {
	Brokers      []string
	Topic        string
	Producer     sarama.AsyncProducer
	SaramaConfig *sarama.Config
}

// NewProducer creates a new producer for the given configuration.
func NewProducer(config *Config) (*Producer, error) {
	producer := &Producer{}

	producer.Brokers = config.Producer.Brokers
	producer.Topic = config.Producer.Topic

	producer.SaramaConfig = sarama.NewConfig()

	producer.SaramaConfig.Producer.RequiredAcks = sarama.NoResponse
	producer.SaramaConfig.Producer.Compression = sarama.CompressionNone

	p, err := sarama.NewAsyncProducer(producer.Brokers, producer.SaramaConfig)
	if err != nil {
		return nil, err
	}

	producer.Producer = p

	return producer, nil
}

// Close stop the Producer and detaches any running sessions.
func (p *Producer) Close() error {
	if err := p.Producer.Close(); err != nil {
		fmt.Println("failed to shut down producer cleanly.", err)
	}
	return nil
}
