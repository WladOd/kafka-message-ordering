package main

import (
	"context"
	"flag"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

const (
	address = "localhost:29092"
	topic   = "myTopic"
)

func main() {
	grpID := flag.String("group", "", "a string")
	partn := flag.Int("partition", 0, "kafka partition")
	flag.Parse()
	if *grpID != "" && *partn != 0 {
		log.Fatalln(`If "GroupID" is specified then "Partition" should NOT be specified for kafka consumer`)
	}

	ctx, cancel := context.WithCancel(context.Background())
	consum := GetUsersKconsumer(ctx, address, *grpID, *partn)
	go consum.Fetch()
	time.Sleep(5 * time.Minute)
	cancel()
}

type consumer struct {
	r   *kafka.Reader
	ctx context.Context
}

func GetUsersKconsumer(ctx context.Context, address, groupID string, ptn int) *consumer {

	return &consumer{
		r: kafka.NewReader(kafka.ReaderConfig{
			Brokers:   []string{address},
			GroupID:   groupID,
			Topic:     topic,
			Partition: ptn,
			// MinBytes: 1e3, // 10KB
			// MaxBytes: 10e6, // 10MB
		}),
		ctx: ctx,
	}
}

func (c *consumer) Fetch() {
	for {
		select {
		case <-c.ctx.Done():
			log.Println("consumer interupted by context")
			return
		default:
			ctx, close := context.WithCancel(c.ctx)
			defer close()

			m, err := c.r.FetchMessage(ctx)
			if err != nil {
				log.Println(`consumer failed to fetch messages: `, err.Error())
				close()
				continue
			}
			log.Printf(`consumer fetch message - "%s" : "%s" - partition "%d" `, string(m.Key), string(m.Value), m.Partition)

			if c.r.Config().GroupID != "" {
				if err := c.r.CommitMessages(ctx, m); err != nil {
					log.Println(`consumer failed to commit messages: `, err.Error())
				}
			}
			close()
		}
	}
}
