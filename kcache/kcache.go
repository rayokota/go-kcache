package kcache

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/emirpasic/gods/maps/treemap"
	"github.com/emirpasic/gods/utils"
	"github.com/rayokota/go-kcache/serde"
	"sync"
)

type CacheUpdateHandler interface {
	update(key interface{}, value interface{})
}

type KCache struct {
	Topic                    string
	DesiredReplicationFactor int32
	DesiredNumPartitions     int32
	GroupId                  string
	ClientId                 string
	CacheUpdateHandler       *CacheUpdateHandler
	KeySerde                 serde.Serde
	ValueSerde               serde.Serde
	LocalCache               *treemap.Map
	Initialized              bool // Arc<AtomicBool>,
	RequireCompact           bool
	InitTimeout              int64
	Timeout                  int64
	BootstrapBrokers         string
	Producer                 *kafka.Producer
	Offsets                  map[int32]kafka.Offset
	CondVar                  *sync.Cond
}

func New(bootstrapBrokers string,
	cacheUpdateHandler *CacheUpdateHandler,
	keySerde serde.Serde,
	valueSerde serde.Serde,
	comparator utils.Comparator) (*KCache, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapBrokers,
	})
	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		return nil, err
	}

	c := new(KCache)
	c.Topic = "_schemas"
	c.DesiredReplicationFactor = 3
	c.DesiredNumPartitions = 1
	c.GroupId = "kafkacache"
	c.ClientId = "kafkacache-reader-schemas"
	c.CacheUpdateHandler = cacheUpdateHandler
	c.KeySerde = keySerde
	c.ValueSerde = valueSerde
	c.LocalCache = treemap.NewWith(comparator)
	c.Initialized = false
	c.RequireCompact = true
	c.InitTimeout = 300000
	c.Timeout = 60000
	c.BootstrapBrokers = bootstrapBrokers
	c.Producer = producer
	c.Offsets = make(map[int32]kafka.Offset)
	c.CondVar = sync.NewCond(&sync.Mutex{})
	return c, nil
}

func (c *KCache) Init() error {
	// TODO create topic
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  c.BootstrapBrokers,
		"group.id":           c.GroupId,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": "false",
	})
	if err != nil {
		return err
	}
	var tpl []kafka.TopicPartition
	for i := int32(0); i < c.DesiredNumPartitions; i++ {
		tpl = append(tpl, kafka.TopicPartition{
			Topic:     &c.Topic,
			Partition: i,
			Offset:    kafka.OffsetBeginning,
		})
	}
	err = consumer.Assign(tpl)
	if err != nil {
		return err
	}
	endOffsets := make(map[int32]kafka.Offset)
	for i := int32(0); i < c.DesiredNumPartitions; i++ {
		_, high, err := consumer.QueryWatermarkOffsets(c.Topic, i, -1)
		if err != nil {
			return err
		}
		endOffsets[i] = kafka.Offset(high) - 1
	}

	go func() {
		for {
			msg, err := consumer.ReadMessage(-1)
			if err != nil {
				fmt.Printf("Consumer error: %v (%v)\n", err, msg)
				continue
			}
			key, err := c.KeySerde.FromBytes(msg.Key)
			if err != nil {
				fmt.Printf("Key deserialization error: %v (%v)\n", err, msg)
				continue
			}
			var value interface{}
			if msg.Value != nil {
				value, err = c.ValueSerde.FromBytes(msg.Value)
				if err != nil {
					fmt.Printf("Value deserialization error: %v (%v)\n", err, msg)
					continue
				}
				c.LocalCache.Put(key, value)
			} else {
				c.LocalCache.Remove(key)
			}
			if c.CacheUpdateHandler != nil {
				(*c.CacheUpdateHandler).update(key, value)
			}
			c.CondVar.L.Lock()
			c.Offsets[msg.TopicPartition.Partition] = msg.TopicPartition.Offset
			c.CondVar.Broadcast()
			c.CondVar.L.Unlock()
		}
	}()

	for i := int32(0); i < c.DesiredNumPartitions; i++ {
		c.waitUntilOffset(i, endOffsets[i])

	}
	c.Initialized = true
	return nil
}

func (c *KCache) waitUntilOffset(partition int32, offset kafka.Offset) {
	if offset < 0 {
		return
	}
	c.CondVar.L.Lock()
	val, _ := c.Offsets[partition]
	for val < offset {
		c.CondVar.Wait()
		val, _ = c.Offsets[partition]
	}
	c.CondVar.L.Unlock()
}

func (c *KCache) Get(key interface{}) (value interface{}, found bool) {
	return c.LocalCache.Get(key)
}

func (c *KCache) Put(key interface{}, value interface{}) (oldValue interface{}, err error) {
	return c.mutate(key, value)
}

func (c *KCache) Delete(key interface{}) (oldValue interface{}, err error) {
	return c.mutate(key, nil)
}

func (c *KCache) mutate(key interface{}, value interface{}) (oldValue interface{}, err error) {
	oldValue, _ = c.LocalCache.Get(key)
	keyBytes, err := c.KeySerde.ToBytes(key)
	if err != nil {
		return
	}
	var valueBytes []byte
	if value != nil {
		valueBytes, err = c.ValueSerde.ToBytes(value)
		if err != nil {
			return
		}
	}
	deliveryChan := make(chan kafka.Event)
	var partition int32 = 0
	message := kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &c.Topic,
			Partition: partition,
		},
		Value: valueBytes,
		Key:   keyBytes,
	}
	err = c.Producer.Produce(&message, deliveryChan)
	if err != nil {
		return
	}
	e := <-deliveryChan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		return nil, m.TopicPartition.Error
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}
	c.waitUntilOffset(partition, m.TopicPartition.Offset)
	return
}

func (c *KCache) Size() int {
	return c.LocalCache.Size()
}

func (c *KCache) Empty() bool {
	return c.LocalCache.Empty()
}
