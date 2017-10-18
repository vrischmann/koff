package koff_test

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/require"
	"github.com/vrischmann/koff"
)

type encoded string

func (e encoded) Encode() ([]byte, error) { return []byte(e), nil }
func (e encoded) Length() int             { return len(e) }

func getClient(t testing.TB) (sarama.Client, func()) {
	broker := sarama.NewMockBroker(t, 1)

	metadataResponse := sarama.NewMockMetadataResponse(t)
	metadataResponse.SetBroker(broker.Addr(), 1)
	metadataResponse.SetLeader("foobar", 0, 1)
	metadataResponse.SetLeader("foobar", 1, 1)

	fetchResponse := sarama.NewMockFetchResponse(t, 1)
	fetchResponse.SetMessage("foobar", 0, 0, encoded("vincent"))

	produceResponse := sarama.NewMockProduceResponse(t)

	offsetResponse := sarama.NewMockOffsetResponse(t)
	offsetResponse.SetOffset("foobar", 0, sarama.OffsetOldest, 500)
	offsetResponse.SetOffset("foobar", 0, sarama.OffsetNewest, 1000)
	offsetResponse.SetOffset("foobar", 1, sarama.OffsetOldest, 5000)
	offsetResponse.SetOffset("foobar", 1, sarama.OffsetNewest, 10000)

	offsetFetchResponse := sarama.NewMockOffsetFetchResponse(t)
	offsetFetchResponse.SetOffset("myConsumerGroup", "foobar", 0, 800, "", sarama.ErrNoError)
	offsetFetchResponse.SetOffset("myConsumerGroup", "foobar", 1, 8000, "", sarama.ErrNoError)

	consumerMetadataResponse := sarama.NewMockConsumerMetadataResponse(t)
	consumerMetadataResponse.SetCoordinator("myConsumerGroup", broker)

	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest":         metadataResponse,
		"FetchRequest":            fetchResponse,
		"ProduceRequest":          produceResponse,
		"OffsetRequest":           offsetResponse,
		"OffsetFetchRequest":      offsetFetchResponse,
		"ConsumerMetadataRequest": consumerMetadataResponse,
	})

	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewManualPartitioner

	client, err := sarama.NewClient([]string{broker.Addr()}, config)
	require.Nil(t, err)

	closeFn := func() {
		require.Nil(t, client.Close())
		broker.Close()
	}

	return client, closeFn
}

func TestGetOffsets(t *testing.T) {
	client, closeFn := getClient(t)
	defer closeFn()

	k := koff.New(client)
	err := k.Init()
	require.Nil(t, err)

	offsets, err := k.GetOldestOffsets("foobar", 0, 1)
	require.Nil(t, err)

	require.Equal(t, 2, len(offsets))
	require.Equal(t, int64(500), offsets[0])
	require.Equal(t, int64(5000), offsets[1])

	offsets, err = k.GetNewestOffsets("foobar", 0, 1)
	require.Nil(t, err)

	require.Equal(t, 2, len(offsets))
	require.Equal(t, int64(999), offsets[0])
	require.Equal(t, int64(9999), offsets[1])
}

func TestGetConsumerGroupOffsets(t *testing.T) {
	client, closeFn := getClient(t)
	defer closeFn()

	k := koff.New(client)
	err := k.Init()
	require.Nil(t, err)

	offsets, err := k.GetConsumerGroupOffsets("myConsumerGroup", "foobar", koff.KafkaOffsetVersion, 0, 1)
	require.Nil(t, err)

	require.Equal(t, 2, len(offsets))
	require.Equal(t, int64(800), offsets[0])
	require.Equal(t, int64(8000), offsets[1])
}

func TestGetDrift(t *testing.T) {
	client, closeFn := getClient(t)
	defer closeFn()

	k := koff.New(client)
	err := k.Init()
	require.Nil(t, err)

	drifts, err := k.GetDrift("myConsumerGroup", "foobar", koff.KafkaOffsetVersion, 0, 1)
	require.Nil(t, err)
	require.Equal(t, 2, len(drifts))
	require.Equal(t, int64(199), drifts[0])
	require.Equal(t, int64(1999), drifts[1])
}
