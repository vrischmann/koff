package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/vrischmann/koff"
)

var (
	flBroker        string
	flConsumerGroup string
	flVersion       koff.OffsetVersion
	flTopic         string
	flPartition     int
	flNewest        bool

	fsGCGO  = flag.NewFlagSet("gcgo", flag.ContinueOnError)
	fsGO    = flag.NewFlagSet("go", flag.ContinueOnError)
	fsDrift = flag.NewFlagSet("drift", flag.ContinueOnError)
)

func init() {
	flag.Usage = printUsage

	flag.StringVar(&flBroker, "b", "", "The broker to use")

	fsGCGO.StringVar(&flConsumerGroup, "c", "", "The consumer group")
	fsGCGO.Var(&flVersion, "V", "The Kafka offset version")
	fsGCGO.StringVar(&flTopic, "t", "", "The topic")
	fsGCGO.IntVar(&flPartition, "p", -1, "The partition")

	fsGO.StringVar(&flTopic, "t", "", "The topic")
	fsGO.IntVar(&flPartition, "p", -1, "The partition")
	fsGO.BoolVar(&flNewest, "n", true, "Get the newest offset instead of the oldest")

	fsDrift.StringVar(&flConsumerGroup, "c", "", "The consumer group")
	fsDrift.Var(&flVersion, "V", "The Kafka offset version")
	fsDrift.StringVar(&flTopic, "t", "", "The topic")
	fsDrift.IntVar(&flPartition, "p", -1, "The partition")
	fsDrift.BoolVar(&flNewest, "n", true, "Compare to the newest offset instead of the oldest")
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage of %s\n", os.Args[0])
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\nSubcommands:\n\nget-consumer-group-offset, gcgo\n")
	fsGCGO.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\nget-offset, go\n")
	fsGO.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\ndrift, d\n")
	fsDrift.PrintDefaults()
}
