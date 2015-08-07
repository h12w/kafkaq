package kafkaq

import "time"

type Job interface {
	Do() error
}

type JobQ struct {
	inputQ    *Q
	consumers []*Q
	doneQ     *Q
	failQ     *Q
	newJob    func() Job
	errCh     chan error
}

type JobQError struct {
	Msg string
	Job Job
	Err error
}

func (e *JobQError) Error() string {
	return e.Msg
}

type JobQConfig struct {
	Addrs         []string
	JobQTopic     string
	DoneQTopic    string
	FailQTopic    string
	ConsumerCount int
	NewJob        func() Job
}

func NewJobQ(config *JobQConfig) (*JobQ, error) {
	// TODO: heartbeat
	errCh := make(chan error)
	inputQ, err := New(&Config{
		KafkaAddrs:     config.Addrs,
		Topic:          config.JobQTopic,
		PartitionCount: config.ConsumerCount,
		ConsumerConfig: ConsumerConfig{
			ConsumerGroup: config.JobQTopic,
		},
	})
	doneQ, err := New(&Config{
		KafkaAddrs:     config.Addrs,
		Topic:          config.DoneQTopic,
		PartitionCount: 1,
		ConsumerConfig: ConsumerConfig{
			ConsumerGroup: config.DoneQTopic,
		},
	})
	failQ, err := New(&Config{
		KafkaAddrs:     config.Addrs,
		Topic:          config.FailQTopic,
		PartitionCount: 1,
		ConsumerConfig: ConsumerConfig{
			ConsumerGroup: config.FailQTopic,
		},
	})
	if err != nil {
		return nil, err
	}
	q := &JobQ{
		inputQ:    inputQ,
		consumers: make([]*Q, config.ConsumerCount),
		doneQ:     doneQ,
		failQ:     failQ,
		newJob:    config.NewJob,
		errCh:     errCh,
	}
	for i := range q.consumers {
		var err error
		q.consumers[i], err = New(&Config{
			KafkaAddrs: config.Addrs,
			Topic:      config.JobQTopic,
			ConsumerConfig: ConsumerConfig{
				ConsumerGroup: config.JobQTopic,
				Partition:     int32(i),
			},
		})
		if err != nil {
			return nil, err
		}
	}
	return q, nil
}

func (p *JobQ) Put(job Job) error {
	return p.inputQ.Put(job)
}

func (p *JobQ) Consume() <-chan error {
	for _, c := range p.consumers {
		go p.process(c)
	}
	return p.errCh
}

func (p *JobQ) process(consumer *Q) {
	for {
		job := p.newJob()
		if err := consumer.Peek(&job); err != nil {
			time.Sleep(time.Second)
			continue
		}
		if err := job.Do(); err != nil {
			p.sendErr("fail to do the job", job, err)
			if err := consumer.PopTo(p.failQ, nil); err != nil {
				p.sendErr("fail to put job into the fail queue", job, err)
			}
			continue
		}
		if err := consumer.PopTo(p.doneQ, nil); err != nil {
			p.sendErr("fail to put job into the done queue", job, err)
		}
	}
}

func (p *JobQ) sendErr(msg string, job Job, err error) {
	p.errCh <- &JobQError{Msg: msg, Job: job, Err: err}
}
