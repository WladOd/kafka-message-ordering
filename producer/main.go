package main

import (
	"context"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

const (
	address = "localhost:29092"
	topic   = "myTopic"
)

var (
	msgs = []kafka.Message{
		{
			Key:   []byte("a"),
			Value: []byte("1"),
		},
		{
			Key:   []byte("b"),
			Value: []byte("2"),
		},
		{
			Key:   []byte("c"),
			Value: []byte("3"),
		},
		{
			Key:   []byte("d"),
			Value: []byte("4"),
		},
		{
			Key:   []byte("a"),
			Value: []byte("5"),
		},
		{
			Key:   []byte("b"),
			Value: []byte("6"),
		},
		{
			Key:   []byte("c"),
			Value: []byte("7"),
		},
		{
			Key:   []byte("d"),
			Value: []byte("8"),
		},
		{
			Key:   []byte("a"),
			Value: []byte("9"),
		},
		{
			Key:   []byte("b"),
			Value: []byte("10"),
		},
		{
			Key:   []byte("c"),
			Value: []byte("11"),
		},
		{
			Key:   []byte("d"),
			Value: []byte("12"),
		},
	}
)

func main() {
   prod := NewKafkaProducer(topic, address)
   prod.Produce(msgs...)
   prod.Close()	
}

type producer struct {
	w   *kafka.Writer
	ctx context.Context
}

func NewKafkaProducer(topic, address string) *producer {
	return &producer{
		w: &kafka.Writer{
			Addr:     kafka.TCP(address),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
			Async:    false,
		},
		ctx: context.Background(),
	}
}

func (kp *producer) Produce(msgs ...kafka.Message) error {
	ctx, cancel := context.WithCancel(kp.ctx)
	defer cancel()

	// Passing a context can prevent the operation from blocking indefinitely.
	switch err := kp.w.WriteMessages(ctx, msgs...).(type) {
	case nil:
	case kafka.WriteErrors:
		for i := range msgs {
			if err[i] != nil {
				// handle the error writing msgs[i]
				log.Println("the specific msg writing error: ", err[i].Error())
			}
		}
	default:
		// handle other errors
		log.Panicln("the general msg writing error: ", err.Error())
	}

	return nil
}

func (kp *producer) Close() error {
	if err := kp.w.Close(); err != nil {
		log.Panicln("the producer closing error: ", err.Error())
		return err
	}
	return nil
}

