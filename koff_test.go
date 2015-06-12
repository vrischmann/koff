package koff_test

import (
	"os"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
	"github.com/vrischmann/koff"
)

func getClient(t testing.TB) sarama.Client {
	addr := os.Getenv("KAFKA_BROKER")
	if addr == "" {
		addr = "localhost:9092"
	}

	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewManualPartitioner

	client, err := sarama.NewClient([]string{addr}, config)
	require.Nil(t, err)

	return client
}

func getProducer(t testing.TB, client sarama.Client) sarama.SyncProducer {
	producer, err := sarama.NewSyncProducerFromClient(client)
	require.Nil(t, err)

	return producer
}

func produce(t testing.TB, partition int32, producer sarama.SyncProducer) {
	msg := sarama.ProducerMessage{
		Topic:     "foobar",
		Partition: partition,
		Value:     sarama.ByteEncoder("blabla"),
	}
	_, _, err := producer.SendMessage(&msg)
	require.Nil(t, err)
}

func TestGetOffsets(t *testing.T) {
	client := getClient(t)
	defer client.Close()

	k := koff.New(client)
	err := k.Init()
	require.Nil(t, err)

	offsets, err := k.GetOldestOffsets("foobar", 0, 1)
	require.Nil(t, err)

	require.Equal(t, 2, len(offsets))
	require.Equal(t, int64(0), offsets[0])
	require.Equal(t, int64(0), offsets[1])

	p := getProducer(t, client)
	produce(t, 0, p)
	produce(t, 1, p)

	offsets, err = k.GetOldestOffsets("foobar", 0, 1)
	require.Nil(t, err)

	require.Equal(t, 2, len(offsets))
	require.Equal(t, int64(0), offsets[0])
	require.Equal(t, int64(0), offsets[1])

	offsets, err = k.GetNewestOffsets("foobar", 0, 1)
	require.Nil(t, err)

	require.Equal(t, 2, len(offsets))
	require.Equal(t, int64(1), offsets[0])
	require.Equal(t, int64(1), offsets[1])
}

func TestGetConsumerGroupOffsets(t *testing.T) {
	client := getClient(t)
	defer client.Close()
	p := getProducer(t, client)
	produce(t, 0, p)
	produce(t, 1, p)

	k := koff.New(client)
	err := k.Init()
	require.Nil(t, err)

	offsets, err := k.GetConsumerGroupOffsets("myConsumerGroup", "foobar", 0, 1)
	require.Nil(t, err)

	require.Equal(t, 2, len(offsets))
	require.Equal(t, int64(0), offsets[0])
	require.Equal(t, int64(0), offsets[1])
}
