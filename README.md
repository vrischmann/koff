koff
====

koff is a small tool to get information about Kafka offsets.

Run `koff --help` to get the help.

Here it is for your convenience:

```
Usage of ./koff

Subcommands:

cgo (consumer group get offset)
  -b="": The broker to use
  -c="": The consumer group
  -p=-1: The partition
  -t="": The topic

go (get offset)
  -b="": The broker to use
  -n=true: Get the newest offset instead of the oldest
  -p=-1: The partition
  -t="": The topic
```
