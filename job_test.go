package kafkaq

import (
	"fmt"
	"testing"
	"time"
)

type testJob struct {
	ID     int
	doneCh chan bool
}

func (j *testJob) Do() error {
	j.doneCh <- true
	return nil
}

func TestJobQ(t *testing.T) {
	doneCh := make(chan bool)
	jobCount := 10
	consumerCount := 3
	jobQTopic, err := zookeeper.NewTopic(consumerCount)
	if err != nil {
		t.Fatal(err)
	}
	defer zookeeper.DeleteTopic(jobQTopic)
	doneQTopic, err := zookeeper.NewTopic(1)
	if err != nil {
		t.Fatal(err)
	}
	defer zookeeper.DeleteTopic(doneQTopic)
	failQTopic, err := zookeeper.NewTopic(1)
	if err != nil {
		t.Fatal(err)
	}
	defer zookeeper.DeleteTopic(failQTopic)
	q, err := NewJobQ(&JobQConfig{
		Addrs:         []string{kafkaHost},
		JobQTopic:     jobQTopic,
		DoneQTopic:    doneQTopic,
		FailQTopic:    failQTopic,
		ConsumerCount: consumerCount,
		NewJob: func() Job {
			return &testJob{doneCh: doneCh}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < jobCount; i++ {
		if err := q.Put(&testJob{ID: i}); err != nil {
			t.Fatal(err)
		}
	}
	errCh := q.Consume()
	go func() {
		for err := range errCh {
			if jobErr, ok := err.(*JobQError); ok {
				t.Fatalf("%v %v", jobErr, jobErr.Err)
			} else {
				t.Fatal(err)
			}
		}
	}()
	go func() {
		doneCount := 0
		for done := range doneCh {
			if done {
				doneCount++
			}
		}
		if doneCount != jobCount {
			t.Fatalf("%d jobs are NOT done.", jobCount-doneCount)
		}
	}()
	time.Sleep(time.Millisecond)
	fmt.Println(zookeeper.DescribeTopic(jobQTopic))
	close(doneCh)
}
