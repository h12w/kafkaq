package kafkaq

import (
	"os"
	"reflect"
	"testing"

	"h12.me/gspec/db/kafka"
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
var zookeeper = kafka.ZooKeeper{Addr: zooKeeperHost}

func TestPutGet(t *testing.T) {
	topic, err := zookeeper.NewTopic(1)
	if err != nil {
		t.Fatal(err)
	}
	defer zookeeper.DeleteTopic(topic)

	q, err := New(&Config{
		KafkaAddrs:     []string{kafkaHost},
		ZooKeeperAddrs: []string{zooKeeperHost},
		Topic:          topic,
		PartitionCount: 1,
		ConsumerConfig: ConsumerConfig{
			ConsumerGroup: topic,
			Partition:     0,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var s string
	{
		if err := q.Put("a"); err != nil {
			t.Fatal(err)
		}
		if err := q.Pop(&s); err != nil {
			t.Fatal(err)
		}
		if s != "a" {
			t.Fatal("expect a but got", s)
		}
	}
	{
		if err := q.Put("a"); err != nil {
			t.Fatal(err)
		}
		if err := q.Put("b"); err != nil {
			t.Fatal(err)
		}
		if err := q.Put("c"); err != nil {
			t.Fatal(err)
		}
		if err := q.Pop(&s); err != nil || s != "a" {
			t.Fatal("expect a but got", s)
		}
		if err := q.Pop(&s); err != nil || s != "b" {
			t.Fatal("expect b but got", s)
		}
		if err := q.Pop(&s); err != nil || s != "c" {
			t.Fatal("expect c but got", s)
		}
	}
}

func TestPutPop(t *testing.T) {
	q := newIntQ(t, "kafka-put-pop", 1, 2, 3)
	defer zookeeper.DeleteTopic(q.Name)
	var i int
	if err := q.Pop(&i); err != nil {
		t.Fatal(err)
	}
	if i != 1 {
		t.Fatalf("expect 1 but got %d", i)
	}
	if err := q.Pop(&i); err != nil {
		t.Fatal(err)
	}
	if i != 2 {
		t.Fatalf("expect 2 but got %d", i)
	}
	if err := q.Pop(&i); err != nil {
		t.Fatal(err)
	}
	if i != 3 {
		t.Fatalf("expect 2 but got %d", i)
	}
}

type testStruct struct {
	I int
	S string
}

func TestEncoding(t *testing.T) {
	q := newQ(t, "kafka-encoding")
	defer zookeeper.DeleteTopic(q.Name)
	s := testStruct{
		I: 1,
		S: "a",
	}
	if err := q.Put(s); err != nil {
		t.Fatal(err)
	}
	var got testStruct
	if err := q.Pop(&got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, s) {
		t.Fatalf("expect %v, got %v", s, got)
	}
}

func TestPeek(t *testing.T) {
	name := "kafka-peek"
	q := newIntQ(t, name, 1, 2)
	defer zookeeper.DeleteTopic(q.Name)
	for j := 0; j < 3; j++ {
		var i int
		if err := q.Peek(&i); err != nil {
			t.Fatal(err)
		}
		if i != 1 {
			t.Fatalf("expect 1 but got %d", i)
		}
	}
	var i int
	if err := q.Pop(&i); err != nil {
		t.Fatal(err)
	}
	if i != 1 {
		t.Fatalf("expect 1 but got %d", i)
	}
	if err := q.Pop(&i); err != nil {
		t.Fatal(err)
	}
	if i != 2 {
		t.Fatalf("expect 2 but got %d", i)
	}
}

func TestResume(t *testing.T) {
	name := "kafka-resume"
	{
		q := newIntQ(t, name, 1, 2)
		defer zookeeper.DeleteTopic(q.Name)
		var i int
		if err := q.Pop(&i); err != nil {
			t.Fatal(err)
		}
		if i != 1 {
			t.Fatalf("expect 1 but got %d", i)
		}
	}
	{
		q := newQ(t, name)
		var i int
		if err := q.Pop(&i); err != nil {
			t.Fatal(err)
		}
		if i != 2 {
			t.Fatalf("expect 2 but got %d", i)
		}
	}
}

func TestPopDiscard(t *testing.T) {
	q := newIntQ(t, "kafka-peek", 1, 2, 3)
	defer zookeeper.DeleteTopic(q.Name)
	if err := q.Pop(nil); err != nil {
		t.Fatal(err)
	}
	var i int
	if err := q.Pop(&i); err != nil {
		t.Fatal(err)
	}
	if i != 2 {
		t.Fatalf("expect 2 but got %d", i)
	}
}

func TestPopTo(t *testing.T) {
	q1 := newIntQ(t, "kafka-popto-1", 1, 2)
	defer zookeeper.DeleteTopic(q1.Name)
	q2 := newIntQ(t, "kafka-popto-2", 3)
	defer zookeeper.DeleteTopic(q2.Name)
	var i int
	if err := q1.PopTo(q2, &i); err != nil {
		t.Fatal(err)
	}
	if i != 1 {
		t.Fatalf("expect 1 but got %d", i)
	}
	if err := q1.Pop(&i); err != nil {
		t.Fatal(err)
	}
	if i != 2 {
		t.Fatalf("expect 2 but got %d", i)
	}
	if err := q2.Pop(&i); err != nil {
		t.Fatal(err)
	}
	if i != 3 {
		t.Fatalf("expect 3 but got %d", i)
	}
	if err := q2.Pop(&i); err != nil {
		t.Fatal(err)
	}
	if i != 1 {
		t.Fatalf("expect 3 but got %d", i)
	}
}

func newQ(t *testing.T, topic string) *Q {
	q, err := New(&Config{
		KafkaAddrs:     []string{kafkaHost},
		Topic:          topic,
		PartitionCount: 1,
		ConsumerConfig: ConsumerConfig{
			ConsumerGroup: topic,
			Partition:     0,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func newIntQ(t *testing.T, name string, is ...int) *Q {
	q := newQ(t, name)
	for _, i := range is {
		if err := q.Put(i); err != nil {
			t.Fatal(err)
		}
	}
	return q
}
