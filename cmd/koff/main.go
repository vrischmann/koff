package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/vrischmann/koff"
)

type command int

const (
	cmdGetOffset command = iota
	cmdGetConsumerGroupOffset
	cmdDrift
)

var (
	topics []string
	cmd    command

	client sarama.Client
)

func initSarama() (err error) {
	config := sarama.NewConfig()
	config.ClientID = "koff"
	config.Consumer.Return.Errors = false

	client, err = sarama.NewClient([]string{flBroker}, config)
	if err != nil {
		return err
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

	if cmd == cmdDrift || cmd == cmdGetConsumerGroupOffset {
		if flConsumerGroup == "" {
			return errors.New("consumer group is not set")
		}
	}

	return nil
}

func getConsumerGroupOffset() (err error) {
	k := koff.New(client)
	if err := k.Init(); err != nil {
		return err
	}

	var offsets map[int32]int64
	{
		partition := int32(flPartition)
		if partition > -1 {
			offsets, err = k.GetConsumerGroupOffsets(flConsumerGroup, flTopic, int16(flVersion), partition)
		} else {
			offsets, err = k.GetConsumerGroupOffsets(flConsumerGroup, flTopic, int16(flVersion))
		}

		if err != nil {
			return err
		}
	}

	var keys []int
	for k, _ := range offsets {
		keys = append(keys, int(k))
	}

	sort.Ints(keys)

	for _, part := range keys {
		fmt.Printf("p:%-6d %-10d\n", part, offsets[int32(part)])
	}

	return nil
}

func getOffset() (err error) {
	k := koff.New(client)
	if err := k.Init(); err != nil {
		return err
	}

	var offsets map[int32]int64
	{
		partition := int32(flPartition)
		if partition > -1 && flNewest {
			offsets, err = k.GetNewestOffsets(flTopic, partition)
		} else if partition > -1 && !flNewest {
			offsets, err = k.GetOldestOffsets(flTopic, partition)
		} else if partition == -1 && flNewest {
			offsets, err = k.GetNewestOffsets(flTopic)
		} else if partition == -1 && !flNewest {
			offsets, err = k.GetOldestOffsets(flTopic)
		}

		if err != nil {
			return err
		}
	}

	var keys []int
	for k, _ := range offsets {
		keys = append(keys, int(k))
	}

	sort.Ints(keys)

	for _, part := range keys {
		fmt.Printf("p:%-6d %-10d\n", part, offsets[int32(part)])
	}

	return nil
}

func getDrift() (err error) {
	k := koff.New(client)
	if err := k.Init(); err != nil {
		return err
	}

	partition := int32(flPartition)

	var availableOffsets map[int32]int64
	{
		if partition > -1 && flNewest {
			availableOffsets, err = k.GetNewestOffsets(flTopic, partition)
		} else if partition > -1 && !flNewest {
			availableOffsets, err = k.GetOldestOffsets(flTopic, partition)
		} else if partition == -1 && flNewest {
			availableOffsets, err = k.GetNewestOffsets(flTopic)
		} else if partition == -1 && !flNewest {
			availableOffsets, err = k.GetOldestOffsets(flTopic)
		}

		if err != nil {
			return err
		}
	}

	var offsets map[int32]int64
	{
		if partition > -1 {
			offsets, err = k.GetConsumerGroupOffsets(flConsumerGroup, flTopic, int16(flVersion), partition)
		} else {
			offsets, err = k.GetConsumerGroupOffsets(flConsumerGroup, flTopic, int16(flVersion))
		}

		if err != nil {
			return err
		}
	}

	var keys []int
	for k, _ := range availableOffsets {
		keys = append(keys, int(k))
	}

	sort.Ints(keys)

	for _, part := range keys {
		o := offsets[int32(part)]
		v := availableOffsets[int32(part)]

		fmt.Printf("p:%-6d %-10d %-10d -> %d", part, v, o, v-o)
		if o != v {
			fmt.Printf("   !!!!\n")
		} else {
			fmt.Printf("\n")
		}
	}

	return nil
}

func gcgoCommand() error {
	if err := fsGCGO.Parse(flag.Args()[1:]); err != nil {
		return err
	}

	if err := checkFlags(); err != nil {
		return err
	}

	if err := initSarama(); err != nil {
		return err
	}
	defer client.Close()

	return getConsumerGroupOffset()
}

func goCommand() error {
	if err := fsGO.Parse(flag.Args()[1:]); err != nil {
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

func driftCommand() error {
	if err := fsDrift.Parse(flag.Args()[1:]); err != nil {
		return err
	}

	if err := checkFlags(); err != nil {
		return err
	}

	if err := initSarama(); err != nil {
		return err
	}
	defer client.Close()

	return getDrift()
}

func main() {
	flag.Parse()

	switch strings.ToLower(flag.Arg(0)) {
	case "get-consumer-group-offset", "gcgo":
		cmd = cmdGetConsumerGroupOffset
		if err := gcgoCommand(); err != nil {
			log.Fatalln(err)
			return
		}
	case "get-offset", "go":
		cmd = cmdGetOffset
		if err := goCommand(); err != nil {
			log.Fatalln(err)
			return
		}
	case "drift", "d":
		cmd = cmdDrift
		if err := driftCommand(); err != nil {
			log.Fatalln(err)
			return
		}
	}

}
