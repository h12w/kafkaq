package kafkaq

import (
	"os"
	"testing"
)

var kafkaHost = func() string {
	h := os.Getenv("KAFKA_HOST")
	if h == "" {
		h = "docker:9092"
	}
	return h
}()

var zooKeeperHost = func() string {
	h := os.Getenv("ZOOKEEPER_HOST")
	if h == "" {
		h = "docker:2181"
	}
	return h
}()

func TestPutGet(t *testing.T) {
	q, err := New(&Config{
		KafkaAddrs:     []string{kafkaHost},
		ZooKeeperAddrs: []string{zooKeeperHost},
		Topic:          "kafkaq",
		ConsumerGroup:  "kafkaq",
	})
	if err != nil {
		t.Fatal(err)
	}
	var s string
	{
		c(t, q.Put("a"))
		c(t, q.Get(&s))
		if s != "a" {
			t.Fatal("expect a but got", s)
		}
	}
	{
		c(t, q.Put("a"))
		c(t, q.Put("b"))
		c(t, q.Put("c"))
		c(t, q.Get(&s))
		if s != "a" {
			t.Fatal("expect a but got", s)
		}
		c(t, q.Get(&s))
		if s != "b" {
			t.Fatal("expect b but got", s)
		}
		c(t, q.Get(&s))
		if s != "c" {
			t.Fatal("expect c but got", s)
		}
	}
}

func c(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}
