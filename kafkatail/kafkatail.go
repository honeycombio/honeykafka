package kafkatail

import (
	"context"
	"log"
	"time"

	"github.com/Shopify/sarama"
)

type Options struct {
	Server    string `long:"server" description:"kafka server" default:"localhost"`
	Port      string `long:"port" description:"kafka port" default:"9092"`
	Topic     string `long:"topic" description:"kafka topic" default:"my_topic"`
	Partition int32  `long:"partition" description:"partition to read from"`
}

// GetChans returns a list of channels but it only ever has one entry - the
// partition on which we're listening.
// TODO listen on multiple channels to multiple partitions
func GetChans(ctx context.Context, options Options) ([]chan string, error) {
	linesChans := make([]chan string, 1, 1)
	lines := make(chan string, 1)
	linesChans[0] = lines

	serverString := options.Server + ":" + options.Port
	// TODO use a reasonable kafka *Config instead of nil for the new consumer
	consumer, err := sarama.NewConsumer([]string{serverString}, nil)
	if err != nil {
		panic(err)
	}

	partitionConsumer, err := consumer.ConsumePartition(
		options.Topic, options.Partition, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}

	go func() {
		log.Printf("consumer started")
		for {
			select {
			case msg := <-partitionConsumer.Messages():
				if msg != nil {
					lines <- string(msg.Value)
				} else {
					time.Sleep(1000 * time.Millisecond)
				}
			case <-ctx.Done():
				// listen for the context's Done channel to clean up and exit
				close(lines)
				if err := partitionConsumer.Close(); err != nil {
					log.Fatalln(err)
				}
				if err := consumer.Close(); err != nil {
					log.Fatalln(err)
				}
				return
			}
		}
	}()

	return linesChans, nil
}
