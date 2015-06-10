package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/vrischmann/koff/Godeps/_workspace/src/github.com/Shopify/sarama"
)

type topicAndPartition struct {
	topic     string
	partition int32
}

var (
	topics []string

	client            sarama.Client
	leaders           = make(map[topicAndPartition]*sarama.Broker)
	offsetCoordinator *sarama.Broker
	partitions        = make(map[string][]int32)
)

func initSarama() (err error) {
	config := sarama.NewConfig()
	config.ClientID = "koff"
	config.Consumer.Return.Errors = false

	client, err = sarama.NewClient([]string{flBroker}, config)
	if err != nil {
		return err
	}

	if topics, err = client.Topics(); err != nil {
		return err
	}

	if err = client.RefreshMetadata(topics...); err != nil {
		return err
	}

	for _, topic := range topics {
		p, err := client.Partitions(topic)
		if err != nil {
			return err
		}
		partitions[topic] = p

		for _, p := range partitions[topic] {
			tp := topicAndPartition{topic, p}

			{
				leader, err := client.Leader(tp.topic, tp.partition)
				if err != nil {
					return err
				}

				if err = leader.Open(nil); err != sarama.ErrAlreadyConnected && err != nil {
					return err
				}

				if _, err = leader.Connected(); err != nil {
					return err
				}

				leaders[tp] = leader
			}
		}
	}

	return nil
}

func getOffsetCoordinator() (err error) {
	if offsetCoordinator, err = client.Coordinator(flConsumerGroup); err != nil {
		return err
	}

	if err = offsetCoordinator.Open(nil); err != sarama.ErrAlreadyConnected && err != nil {
		return nil
	}

	if _, err = offsetCoordinator.Connected(); err != nil {
		return err
	}

	return nil
}

func fetchConsumerGroupOffsets(consumerGroup string, topic string, partitions ...int32) (res map[int32]int64, err error) {
	res = make(map[int32]int64)
	for _, p := range partitions {
		req := &sarama.OffsetFetchRequest{
			ConsumerGroup: consumerGroup,
			Version:       1,
		}
		req.AddPartition(topic, p)

		resp, err := offsetCoordinator.FetchOffset(req)
		if err != nil {
			return nil, err
		}

		block := resp.Blocks[topic][p]
		if block.Err != sarama.ErrNoError {
			return nil, block.Err
		}

		res[p] = resp.Blocks[topic][p].Offset
	}

	return
}

func fetchAvailableOffsets(topic string, partitions ...int32) (res map[int32]int64, err error) {
	var offset int64

	if flNewest {
		offset = sarama.OffsetNewest
	} else {
		offset = sarama.OffsetOldest
	}

	res = make(map[int32]int64)
	for _, p := range partitions {
		req := &sarama.OffsetRequest{}
		req.AddBlock(flTopic, p, offset, 1)

		tp := topicAndPartition{topic, p}

		resp, err := leaders[tp].GetAvailableOffsets(req)
		if err != nil {
			return nil, err
		}

		block := resp.GetBlock(flTopic, p)
		if block.Err != sarama.ErrNoError {
			return nil, block.Err
		}

		res[p] = block.Offsets[0]
	}

	return
}

func getConsumerOffset() error {
	if err := getOffsetCoordinator(); err != nil {
		return err
	}

	partition := int32(flPartition)

	var offsets map[int32]int64
	var err error
	if partition > -1 {
		offsets, err = fetchConsumerGroupOffsets(flConsumerGroup, flTopic, partition)
		if err != nil {
			return err
		}

		fmt.Printf("p:%-4d %d\n", partition, offsets[partition])

		return nil
	}

	offsets, err = fetchConsumerGroupOffsets(flConsumerGroup, flTopic, partitions[flTopic]...)
	if err != nil {
		return err
	}

	for p, offset := range offsets {
		fmt.Printf("p:%-4d %d\n", p, offset)
	}

	return nil
}

func getOffset() (err error) {
	partition := int32(flPartition)

	var offsets map[int32]int64
	if partition > -1 {
		offsets, err = fetchAvailableOffsets(flTopic, partition)
		if err != nil {
			return err
		}

		fmt.Printf("p:%-4d %d\n", partition, offsets[partition])

		return nil
	}

	offsets, err = fetchAvailableOffsets(flTopic, partitions[flTopic]...)
	if err != nil {
		return err
	}

	for p, offset := range offsets {
		fmt.Printf("p:%-4d %d\n", p, offset)
	}

	return nil
}

func checkFlags() error {
	if flBroker == "" {
		return errors.New("broker is not set")
	}

	if flTopic == "" {
		return errors.New("topic is not set")
	}

	return nil
}

func cgoCommand() error {
	if err := fsConsumerGroup.Parse(flag.Args()[1:]); err != nil {
		return err
	}

	if err := checkFlags(); err != nil {
		return err
	}

	if err := initSarama(); err != nil {
		return err
	}
	defer client.Close()

	return getConsumerOffset()
}

func goCommand() error {
	if err := fsGetOffset.Parse(flag.Args()[1:]); err != nil {
		return err
	}

	if err := checkFlags(); err != nil {
		return err
	}

	if err := initSarama(); err != nil {
		return err
	}
	defer client.Close()

	return getOffset()
}

func main() {
	flag.Parse()

	switch strings.ToLower(flag.Arg(0)) {
	case "cgo":
		if err := cgoCommand(); err != nil {
			log.Fatalln(err)
			return
		}
	case "go":
		if err := goCommand(); err != nil {
			log.Fatalln(err)
			return
		}
	}

}
