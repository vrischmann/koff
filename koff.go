package koff

import (
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
)

type topicAndPartition struct {
	topic     string
	partition int32
}

// Koff provides method to get and compare offsets of consumer groups.
type Koff struct {
	client sarama.Client

	pMu        sync.RWMutex
	partitions map[string][]int32
}

// New creates a new Koff structure.
func New(client sarama.Client) *Koff {
	return &Koff{
		client:     client,
		partitions: make(map[string][]int32),
	}
}

// Init initializes the state of the Koff instance.
//
// It queries the Kafka cluster for a list of topics and refreshes the metadata for each topic.
func (k *Koff) Init() error {
	topics, err := k.client.Topics()
	if err != nil {
		return err
	}

	if err := k.client.RefreshMetadata(topics...); err != nil {
		return err
	}

	k.pMu.Lock()
	defer k.pMu.Unlock()

	for _, topic := range topics {
		p, err := k.client.Partitions(topic)
		if err != nil {
			return err
		}
		k.partitions[topic] = p
	}

	return nil
}

func (k *Koff) getOffsetCoordinator(consumerGroup string) (*sarama.Broker, error) {
	if err := k.client.RefreshCoordinator(consumerGroup); err != nil {
		return nil, err
	}

	offsetCoordinator, err := k.client.Coordinator(consumerGroup)
	if err != nil {
		return nil, err
	}

	if err = offsetCoordinator.Open(nil); err != sarama.ErrAlreadyConnected && err != nil {
		return offsetCoordinator, nil
	}

	if _, err = offsetCoordinator.Connected(); err != nil {
		return nil, err
	}

	return offsetCoordinator, nil
}

// OffsetInAvailableRange check that the provided offset is in the available range of the topic and partitions.
//
// If multiple partitions are provided, the offset is checked for all partitions.
func (k *Koff) OffsetInAvailableRange(topic string, offset int64, partitions ...int32) (bool, error) {
	return false, nil
}

func (k *Koff) getOffset(topic string, offset int64, partitions ...int32) (res map[int32]int64, err error) {
	if len(partitions) <= 0 {
		k.pMu.RLock()
		partitions = k.partitions[topic]
		k.pMu.RUnlock()
	}

	if len(partitions) > len(k.partitions[topic]) {
		return nil, fmt.Errorf("topic '%s' has only %d partitions", topic, len(k.partitions[topic]))
	}

	res = make(map[int32]int64)
	for _, p := range partitions {
		req := &sarama.OffsetRequest{}
		req.AddBlock(topic, p, offset, 1)

		mkerr := func(err error) error {
			return fmt.Errorf("unable to get available offset for (%s, %d) offset %d. err=%v", topic, p, offset, err)
		}

		if err := k.client.RefreshMetadata(topic); err != nil {
			return nil, mkerr(err)
		}

		leader, err := k.client.Leader(topic, p)
		if err != nil {
			return nil, mkerr(err)
		}

		resp, err := leader.GetAvailableOffsets(req)
		if err != nil {
			return nil, fmt.Errorf("unable to get available offset for (%s, %d) offset %d. err=%v", topic, p, offset, err)
		}

		block := resp.GetBlock(topic, p)
		if block.Err != sarama.ErrNoError {
			return nil, fmt.Errorf("unable to get available offset for (%s, %d) offset %d. err=%v", topic, p, offset, block.Err)
		}

		res[p] = block.Offsets[0]
	}

	return
}

// GetOldestOffsets retrieves the oldest available offsets for each partitions of the provided topic.
//
// Returns a map of partitions to offset.
func (k *Koff) GetOldestOffsets(topic string, partitions ...int32) (res map[int32]int64, err error) {
	return k.getOffset(topic, sarama.OffsetOldest, partitions...)
}

// GetNewestOffsets retrieves the newest available offsets for each partitions of the provided topic.
//
// Returns a map of partitions to offset.
func (k *Koff) GetNewestOffsets(topic string, partitions ...int32) (map[int32]int64, error) {
	return k.getOffset(topic, sarama.OffsetNewest, partitions...)
}

type OffsetVersion int16

const (
	ZKOffsetVersion    OffsetVersion = 0
	KafkaOffsetVersion OffsetVersion = 1
)

func (v *OffsetVersion) Set(s string) error {
	switch s {
	case "0":
		*v = ZKOffsetVersion
	case "1":
		*v = KafkaOffsetVersion
	default:
		return fmt.Errorf("%q unknown offset version", s)
	}
	return nil
}

func (v OffsetVersion) String() string {
	switch v {
	case ZKOffsetVersion:
		return "0"
	case KafkaOffsetVersion:
		return "1"
	default:
		return "unknown"
	}
}

// GetConsumerGroupOffsets retrieves the last committed offsets for the given consumer group.
// Returns a map of partitions to offset.
func (k *Koff) GetConsumerGroupOffsets(consumerGroup, topic string, version OffsetVersion, partitions ...int32) (map[int32]int64, error) {
	offsetCoordinator, err := k.getOffsetCoordinator(consumerGroup)
	if err != nil {
		return nil, fmt.Errorf("unable to init offset coordinator. err=%v", err)
	}

	if len(partitions) <= 0 {
		k.pMu.RLock()
		partitions = k.partitions[topic]
		k.pMu.RUnlock()
	}

	res := make(map[int32]int64)
	for _, p := range partitions {
		req := &sarama.OffsetFetchRequest{
			ConsumerGroup: consumerGroup,
			Version:       int16(version),
		}
		req.AddPartition(topic, p)

		resp, err := offsetCoordinator.FetchOffset(req)
		if err != nil {
			return nil, fmt.Errorf("unable to fetch offset of (%s, %d, %d). err=%v", topic, p, version, err)
		}

		block := resp.Blocks[topic][p]
		if block.Err != sarama.ErrNoError {
			return nil, fmt.Errorf("unable to fetch offset of (%s, %d, %d). err=%v", topic, p, version, block.Err)
		}

		res[p] = resp.Blocks[topic][p].Offset
	}

	return res, nil
}

// GetDrift computes the drift between the last comitted offsets of a consumer group and the newest offsets available for a topic and partition.
//
// Returns a map of partitions to offset.
func (k *Koff) GetDrift(consumerGroup, topic string, version OffsetVersion, partitions ...int32) (map[int32]int64, error) {
	availableOffsets, err := k.GetNewestOffsets(topic, partitions...)
	if err != nil {
		return nil, fmt.Errorf("unable to get newest offsets. err=%v", err)
	}

	cgroupOffsets, err := k.GetConsumerGroupOffsets(consumerGroup, topic, version, partitions...)
	if err != nil {
		return nil, fmt.Errorf("unable to get consumer group offsets. err=%v", err)
	}

	res := make(map[int32]int64)
	for k, v := range cgroupOffsets {
		res[k] = availableOffsets[k] - v
	}

	return res, nil
}
