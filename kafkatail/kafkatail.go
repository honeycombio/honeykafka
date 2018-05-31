package kafkatail

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/cenk/backoff"
	"github.com/rubyist/circuitbreaker"
	"log"
	"time"
)

type Options struct {
	Server         string `long:"server" description:"kafka server" default:"localhost"`
	Port           string `long:"port" description:"kafka port" default:"9092"`
	Topic          string `long:"topic" description:"kafka topic" default:"my_topic"`
	Partition      int32  `long:"partition" description:"partition to read from"`
	StartingOffset int64  `long:"offset" description:"offset to start from" default:"-1"`
}

// GetChans returns a list of channels but it only ever has one entry - the
// partition on which we're listening.
// TODO listen on multiple channels to multiple partitions
func GetChans(ctx context.Context, options Options) ([]chan string, error) {
	lines := make(chan string, 1)

	linesChan := make([]chan string, 1, 1)
	linesChan[0] = lines

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	brokers := []string{options.Server + ":" + options.Port}

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, err
	}

	lastSuccess := options.StartingOffset

	// TODO: Make these configurable
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = 2 * time.Second
	expBackoff.MaxInterval = 30 * time.Second
	expBackoff.MaxElapsedTime = 5 * time.Minute

	breaker := circuit.NewConsecutiveBreaker(10)
	breaker.BackOff = expBackoff

	partitionConsumer, err := consumer.ConsumePartition(options.Topic, options.Partition, lastSuccess)

	if err != nil {
		log.Printf("Error starting PartitionConsumer for topic %v partition %d "+
			"with offset %d\n", options.Topic, options.Partition, options.StartingOffset)
		return nil, err
	}

	log.Printf("Started PartitionConsumer for topic %v partition %d "+
		"with offset %d\n", options.Topic, options.Partition, options.StartingOffset)

	go func() {
		defer func() {
			log.Printf("Stopping consumers for topic %v partition %d; "+
				"last successfully read offset %d\n", options.Topic, options.Partition, lastSuccess)

			close(lines)

			err := partitionConsumer.Close()
			if err != nil {
				log.Fatalf("Error shutting down partition consumer for topic %v partition %d\n",
					options.Topic, options.Partition)
			}

			err = consumer.Close()
			if err != nil {
				log.Fatalf("Error shutting down consumer for brokers %v\n", brokers)
				panic(err)
			}
		}()

		for {
			if breaker.Ready() {
				select {
				case msg, open := <-partitionConsumer.Messages():
					if !open {
						break
					} else if msg != nil {
						lastSuccess = msg.Offset
						lines <- string(msg.Value)
						breaker.Success()
					}
				case <-partitionConsumer.Errors():
					breaker.Fail()
				case <-ctx.Done():
					break
				}
			} else if expBackoff.NextBackOff() == backoff.Stop {
				log.Println("Maximum number of retries exceeded, failing")
			} else {
				select {
					case <- ctx.Done():
						break
					case <- time.After(expBackoff.NextBackOff()):
						continue
				}
			}
		}
	}()

	return linesChan, err
}
