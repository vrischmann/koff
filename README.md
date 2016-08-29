koff
====

[![Build Status](https://travis-ci.org/vrischmann/koff.svg?branch=master)](https://travis-ci.org/vrischmann/koff)
[![GoDoc](https://godoc.org/github.com/vrischmann/koff?status.svg)](https://godoc.org/github.com/vrischmann/koff)

koff is a small tool to get information about Kafka offsets.

Run `koff --help` to get the help.

Here it is for your convenience:

```
Usage of ./koff
  -b="": The broker to use

Subcommands:

get-consumer-group-offset, gcgo
  -V=1: The Kafka offset version
  -c="": The consumer group
  -p=-1: The partition
  -t="": The topic

get-offset, go
  -n=true: Get the newest offset instead of the oldest
  -p=-1: The partition
  -t="": The topic

drift, d
  -V=1: The Kafka offset version
  -c="": The consumer group
  -n=true: Compare to the newest offset instead of the oldest
  -p=-1: The partition
  -t="": The topic

```
