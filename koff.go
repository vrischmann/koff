package koff

import (
	"fmt"

	"github.com/Shopify/sarama"
)

type topicAndPartition struct {
	topic     string
	partition int32
}

type Koff struct {
	client sarama.Client

	partitions         map[string][]int32
	leaders            map[topicAndPartition]*sarama.Broker
	offsetCoordinators map[string]*sarama.Broker
}

func New(client sarama.Client) *Koff {
	return &Koff{
		client:             client,
		partitions:         make(map[string][]int32),
		leaders:            make(map[topicAndPartition]*sarama.Broker),
		offsetCoordinators: make(map[string]*sarama.Broker),
	}
}

func (k *Koff) Init() error {
	topics, err := k.client.Topics()
	if err != nil {
		return err
	}

	if err := k.client.RefreshMetadata(topics...); err != nil {
		return err
	}

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

func (k *Koff) iniOffsetCoordinator(consumerGroup string) (err error) {
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

	k.offsetCoordinators[consumerGroup] = offsetCoordinator

	return nil
}

func (k *Koff) OffsetInAvailableRange(topic string, partitions ...int32) (bool, error) {
	return false, nil
}

func (k *Koff) getOffset(topic string, offset int64, partitions ...int32) (res map[int32]int64, err error) {
	res = make(map[int32]int64)

	if len(partitions) <= 0 {
		partitions = k.partitions[topic]
	}

	if len(partitions) > len(k.partitions[topic]) {
		return nil, fmt.Errorf("topic '%s' has only %d partitions", len(k.partitions[topic]))
	}

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

func (k *Koff) GetOldestOffsets(topic string, partitions ...int32) (res map[int32]int64, err error) {
	return k.getOffset(topic, sarama.OffsetOldest, partitions...)
}

func (k *Koff) GetNewestOffsets(topic string, partitions ...int32) (map[int32]int64, error) {
	return k.getOffset(topic, sarama.OffsetNewest, partitions...)
}

func (k *Koff) GetConsumerGroupOffsets(consumerGroup, topic string, version int16, partitions ...int32) (map[int32]int64, error) {
	if err := k.iniOffsetCoordinator(consumerGroup); err != nil {
		return nil, err
	}

	if len(partitions) <= 0 {
		partitions = k.partitions[topic]
	}

	res := make(map[int32]int64)
	for _, p := range partitions {
		req := &sarama.OffsetFetchRequest{
			ConsumerGroup: consumerGroup,
			Version:       version,
		}
		req.AddPartition(topic, p)

		resp, err := k.offsetCoordinators[consumerGroup].FetchOffset(req)
		if err != nil {
			return nil, err
		}

		block := resp.Blocks[topic][p]
		if block.Err != sarama.ErrNoError {
			return nil, block.Err
		}

		res[p] = resp.Blocks[topic][p].Offset + 1 // the offset we fetch is the last committed we need the next to fetch
	}

	return res, nil
}

func (k *Koff) GetDrift(consumerGroup, topic string, version int16, partitions ...int32) (map[int32]int64, error) {
	availableOffsets, err := k.GetNewestOffsets(topic, partitions...)
	if err != nil {
		return nil, err
	}

	cgroupOffsets, err := k.GetConsumerGroupOffsets(consumerGroup, topic, version, partitions...)
	if err != nil {
		return nil, err
	}

	res := make(map[int32]int64)
	for k, v := range cgroupOffsets {
		res[k] = availableOffsets[k] - v
	}

	return res, nil
}
