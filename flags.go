package main

import (
	"flag"
	"fmt"
	"os"
)

var (
	flBroker        string
	flConsumerGroup string
	flTopic         string
	flPartition     int
	flNewest        bool

	fsConsumerGroup = flag.NewFlagSet("cgo", flag.ContinueOnError)
	fsGetOffset     = flag.NewFlagSet("go", flag.ContinueOnError)
)

func init() {
	flag.Usage = printUsage

	fsConsumerGroup.StringVar(&flBroker, "b", "", "The broker to use")
	fsConsumerGroup.StringVar(&flConsumerGroup, "c", "", "The consumer group")
	fsConsumerGroup.StringVar(&flTopic, "t", "", "The topic")
	fsConsumerGroup.IntVar(&flPartition, "p", -1, "The partition")

	fsGetOffset.StringVar(&flBroker, "b", "", "The broker to use")
	fsGetOffset.StringVar(&flTopic, "t", "", "The topic")
	fsGetOffset.IntVar(&flPartition, "p", -1, "The partition")
	fsGetOffset.BoolVar(&flNewest, "n", true, "Get the newest offset instead of the oldest")
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage of %s\n", os.Args[0])
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\nSubcommands:\n\ncgo (consumer group get offset)\n")
	fsConsumerGroup.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\ngo (get offset)\n")
	fsGetOffset.PrintDefaults()
}
