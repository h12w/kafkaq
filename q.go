package kafkaq

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/stealthly/siesta"
)

func init() {
	//sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
}

type Config struct {
	KafkaAddrs     []string
	ZooKeeperAddrs []string
	Topic          string
	ConsumerConfig
}

type ConsumerConfig struct {
	ConsumerGroup string
	Partition     int32
}

type Q struct {
	Name string
	*Producer
	*Consumer
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
		Name:     config.Topic,
		Producer: p,
		Consumer: c,
	}, nil
}

func (q *Q) PopTo(o *Q, v interface{}) error {
	if v == nil {
		buf, err := q.PeekBytes()
		if err != nil {
			return err
		}
		if err := o.PutBytes(buf); err != nil {
			return err
		}
		return q.Commit()
	}
	if err := q.Peek(v); err != nil {
		return err
	}
	if err := o.Put(v); err != nil {
		return err
	}
	return q.Commit()
}

type Producer struct {
	p     sarama.SyncProducer
	topic string
}

func NewProducer(config *Config) (*Producer, error) {
	scfg := sarama.NewConfig()
	scfg.Producer.Partitioner = sarama.NewRandomPartitioner
	producer, err := sarama.NewSyncProducer(config.KafkaAddrs, scfg)
	if err != nil {
		return nil, err
	}
	return &Producer{
		p:     producer,
		topic: config.Topic,
	}, nil
}

func (p *Producer) Put(v interface{}) error {
	buf, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return p.PutBytes(buf)
}

func (p *Producer) PutBytes(buf []byte) error {
	message := &sarama.ProducerMessage{
		Topic:     p.topic,
		Partition: -1,
		Value:     sarama.ByteEncoder(buf),
	}
	_, _, err := p.p.SendMessage(message)
	return err
}

type Consumer struct {
	c     siesta.Connector
	topic string
	ConsumerConfig
	offset int64
}

func NewConsumer(config *Config) (*Consumer, error) {
	scfg := siesta.NewConnectorConfig()
	scfg.BrokerList = config.KafkaAddrs
	scfg.FetchMaxWaitTime = 3600 * 1000 // TODO: loop between timeout
	c, err := siesta.NewDefaultConnector(scfg)
	if err != nil {
		return nil, err
	}
	offset, err := c.GetOffset(config.ConsumerGroup, config.Topic, config.Partition)
	if err != nil {
		// TODO: handle error properly
		offset = 0
	}
	return &Consumer{
		c:              c,
		topic:          config.Topic,
		ConsumerConfig: config.ConsumerConfig,
		offset:         offset,
	}, nil
}

func (c *Consumer) Pop(v interface{}) error {
	if v != nil {
		err := c.Peek(v)
		if err != nil {
			return err
		}
	}
	fmt.Printf("pop %#v\n", v)
	return c.Commit()
}

func (c *Consumer) Commit() error {
	c.offset++
	c.c.CommitOffset(c.topic, c.topic, c.Partition, c.offset)
	return nil
}

func (c *Consumer) Peek(v interface{}) error {
	buf, err := c.PeekBytes()
	if err != nil {
		return err
	}
	return json.Unmarshal(buf, v)
}

func (c *Consumer) PeekBytes() ([]byte, error) {
	r, err := c.c.Fetch(c.topic, c.Partition, c.offset)
	if err != nil {
		return nil, err
	}
	messages := r.Data[c.topic][c.Partition].Messages
	for _, msg := range messages {
		if msg.Offset == c.offset {
			value := messages[0].Message.Value
			return value, nil
		}
	}
	// TODO: handle buffer
	return nil, errors.New("no data")
}
