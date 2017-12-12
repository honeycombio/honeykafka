package kafkatail

import (
	"log"
	"os"
	"os/signal"
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
func GetChans(options Options) ([]chan string, error) {
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

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	go func() {
		for {
			select {
			case msg := <-partitionConsumer.Messages():
				if msg != nil {
					log.Printf("Consumed message: %+v\n", msg)
					lines <- string(msg.Value)
				} else {
					log.Printf("got nil message\n")
					time.Sleep(1000 * time.Millisecond)
				}
			case <-signals:
				// clean up and exit
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
