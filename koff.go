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

	pMu                sync.RWMutex
	partitions         map[string][]int32
	lMu                sync.RWMutex
	leaders            map[topicAndPartition]*sarama.Broker
	oMu                sync.RWMutex
	offsetCoordinators map[string]*sarama.Broker
}

// New creates a new Koff structure.
func New(client sarama.Client) *Koff {
	return &Koff{
		client:             client,
		partitions:         make(map[string][]int32),
		leaders:            make(map[topicAndPartition]*sarama.Broker),
		offsetCoordinators: make(map[string]*sarama.Broker),
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
	k.lMu.Lock()

	defer func() {
		k.pMu.Unlock()
		k.lMu.Unlock()
	}()

	for _, topic := range topics {
		p, err := k.client.Partitions(topic)
		if err != nil {
			return err
		}
		k.partitions[topic] = p

		for _, p := range k.partitions[topic] {
			tp := topicAndPartition{topic, p}

			{
				leader, err := k.client.Leader(tp.topic, tp.partition)
				if err != nil {
					return err
				}

				if err = leader.Open(nil); err != sarama.ErrAlreadyConnected && err != nil {
					return err
				}

				if _, err = leader.Connected(); err != nil {
					return err
				}

				k.leaders[tp] = leader
			}
		}
	}

	return nil
}

func (k *Koff) initOffsetCoordinator(consumerGroup string) (err error) {
	offsetCoordinator, err := k.client.Coordinator(consumerGroup)
	if err != nil {
		return err
	}

	if err = offsetCoordinator.Open(nil); err != sarama.ErrAlreadyConnected && err != nil {
		return nil
	}

	if _, err = offsetCoordinator.Connected(); err != nil {
		return err
	}

	k.oMu.Lock()
	defer k.oMu.Unlock()

	k.offsetCoordinators[consumerGroup] = offsetCoordinator

	return nil
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

	k.lMu.RLock()
	defer k.lMu.RUnlock()

	res = make(map[int32]int64)
	for _, p := range partitions {
		req := &sarama.OffsetRequest{}
		req.AddBlock(topic, p, offset, 1)

		tp := topicAndPartition{topic, p}

		resp, err := k.leaders[tp].GetAvailableOffsets(req)
		if err != nil {
			return nil, err
		}

		block := resp.GetBlock(topic, p)
		if block.Err != sarama.ErrNoError {
			return nil, block.Err
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

// GetConsumerGroupOffsets retrieves the last committed offsets for the given consumer group.
//
// Version is the version you use when committing:
// - 0 means stored in ZooKeeper.
// - > 1 means stored in Kafka itself.
//
// Returns a map of partitions to offset.
func (k *Koff) GetConsumerGroupOffsets(consumerGroup, topic string, version int16, partitions ...int32) (map[int32]int64, error) {
	if err := k.initOffsetCoordinator(consumerGroup); err != nil {
		return nil, fmt.Errorf("unable to init offset coordinator. err=%v", err)
	}

	if len(partitions) <= 0 {
		k.pMu.RLock()
		partitions = k.partitions[topic]
		k.pMu.RUnlock()
	}

	k.oMu.RLock()
	defer k.oMu.RUnlock()

	res := make(map[int32]int64)
	for _, p := range partitions {
		req := &sarama.OffsetFetchRequest{
			ConsumerGroup: consumerGroup,
			Version:       version,
		}
		req.AddPartition(topic, p)

		resp, err := k.offsetCoordinators[consumerGroup].FetchOffset(req)
		if err != nil {
			return nil, fmt.Errorf("unable to fetch offset of (%s, %d, %d). err=%v", topic, p, version, err)
		}

		block := resp.Blocks[topic][p]
		if block.Err != sarama.ErrNoError {
			return nil, fmt.Errorf("unable to fetch offset of (%s, %d, %d). err=%v", topic, p, version, block.Err)
		}

		res[p] = resp.Blocks[topic][p].Offset + 1 // the offset we fetch is the last committed we need the next to fetch
	}

	return res, nil
}

// GetDrift computes the drift between the last comitted offsets of a consumer group and the newest offsets available for a topic and partition.
//
// Returns a map of partitions to offset.
func (k *Koff) GetDrift(consumerGroup, topic string, version int16, partitions ...int32) (map[int32]int64, error) {
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
