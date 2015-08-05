package kafkaq

import (
	"encoding/json"
	"errors"
	"log"
	"os"

	"github.com/Shopify/sarama"
)

func init() {
	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
}

type Config struct {
	KafkaAddrs     []string
	ZooKeeperAddrs []string
	ConsumerGroup  string
	Topic          string
}

type Q struct {
	p *Producer
	c *Consumer
}

func New(config *Config) (*Q, error) {
	if config.Topic == "" {
		return nil, errors.New("empty topic")
	}
	p, err := NewProducer(config)
	if err != nil {
		return nil, err
	}
	c, err := NewConsumer(config)
	if err != nil {
		return nil, err
	}
	return &Q{
		p: p,
		c: c,
	}, nil
}

func (q *Q) Put(v interface{}) error {
	return q.p.Send(v)
}

func (q *Q) Get(v interface{}) error {
	return q.c.Get(v)
}

type Producer struct {
	p     sarama.SyncProducer
	topic string
}

func NewProducer(config *Config) (*Producer, error) {
	scfg := sarama.NewConfig()
	scfg.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	producer, err := sarama.NewSyncProducer(config.KafkaAddrs, scfg)
	if err != nil {
		return nil, err
	}
	return &Producer{
		p:     producer,
		topic: config.Topic,
	}, nil
}

func (p *Producer) Send(v interface{}) error {
	buf, err := json.Marshal(v)
	if err != nil {
		return err
	}
	message := &sarama.ProducerMessage{
		Topic:     p.topic,
		Partition: -1,
		Value:     sarama.ByteEncoder(buf),
	}
	_, _, err = p.p.SendMessage(message)
	return err
}

type Consumer struct {
}

func NewConsumer(config *Config) (*Consumer, error) {
	return &Consumer{}, nil
}

func (c *Consumer) Get(v interface{}) error {
	return nil
}
