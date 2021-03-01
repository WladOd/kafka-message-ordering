package main

import (
	"context"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	
}

type producer struct {
	w      *kafka.Writer
	ctx   context.Context
}

func NewKafkaProducer(topic, address string) *producer {
	return &producer{
		w: &kafka.Writer{
			Addr:     kafka.TCP(address),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
		ctx:  context.Background(),
	}
}

func (kp *producer) Produce(msgs ...kafka.Message) bool {
	ctx, close := context.WithCancel(kp.ctx)
	defer close()

	err := kp.w.WriteMessages(ctx, msgs...)
	if err != nil {
		return false
	}

	if err := kp.w.Close(); err != nil {
		return false
	}
	return true
}
func (kp *producer) EnsureTopic(address, topic string) bool {
	ctx, close := context.WithCancel(kp.ctx)
	defer close()

	_, err := kafka.DialLeader(ctx, "tcp", address, topic, 0)
	if err != nil {
		return false
	}
	return true
}