package kcache

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
)

type KCache struct {
	Topic string
	DesiredReplicationFactor int32
	DesiredNumPartitions int32
	GroupId string
	ClientId string
	CacheUpdateHandler string // Option<Arc<CacheUpdateHandler<K, V>>>,
	LocalCache *treemap.Map
	Initialized string // Arc<AtomicBool>,
	RequireCompact bool
	InitTimeout int64
	Timeout int64
	BootstrapBrokers string
	Producer *kafka.Producer
	Offsets map[int32]int64
}

func NewWith(bootstrap_brokers string, cache_update_handler string, comparator utils.Comparator) (*KCache, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrap_brokers})
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		return nil, err
	}

	c := new(KCache)
	c.Topic = "_schemas"
	c.CacheUpdateHandler = cache_update_handler
	c.LocalCache = treemap.NewWith(comparator)
	c.BootstrapBrokers = bootstrap_brokers
	c.Producer = p
	c.Offsets = make(map[int32]int64)
	return c, nil
}
