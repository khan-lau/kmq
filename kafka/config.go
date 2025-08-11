package kafka

import (
	"time"

	"github.com/IBM/sarama"
	"github.com/khan-lau/kutils/container/klists"
)

const (
	//  OffsetNewest 表示分区的头部偏移量, 也就是将要被分配给下一条将要生产到该分区的消息的偏移量.
	// 你可以将该值传递给客户端的 GetOffset 方法来获取该偏移量, 或者在调用 ConsumePartition 时使用
	// 该偏移量开始消费最新消息。
	OffsetNewest int64 = -1

	//   OffsetOldest 表示分区上最早可用的偏移量, 也就是说, 你可以将该值传递给客户端的 GetOffset
	// 方法来获取该偏移量，或者在调用 ConsumePartition 时使用该偏移量开始消费最早的可用消息。
	OffsetOldest int64 = -2
)

const (
	kafka_tag = "kafka"
)

/////////////////////////////////////////////////////////////

type ErrorCallbackFunc func(err error)
type EventCallbackFunc func(event interface{})

/////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////

type KafkaMessage struct {
	Topic     string
	Partition int32
	Offset    int64
	Headers   []sarama.RecordHeader
	Key       []byte
	Value     []byte

	session sarama.ConsumerGroupSession
}

// 确认当前消息seq之前的所有消息
func (that *KafkaMessage) Ack() error {
	if that.session == nil {
		return nil
	}
	that.session.MarkOffset(that.Topic, that.Partition, that.Offset+1, "")
	return nil
}

/////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////

type Topic struct {
	Name      string
	Partition map[int32]int64
}

func NewTopic(name string) *Topic {
	partition := make(map[int32]int64)
	partition[0] = OffsetNewest
	return &Topic{
		Name:      name,
		Partition: partition,
	}
}

func (that *Topic) SetOffset(partition int32, offset int64) *Topic {
	if that.Partition == nil {
		that.Partition = make(map[int32]int64)
	}
	if offset < -2 {
		offset = OffsetOldest
	}
	that.Partition[partition] = offset
	return that
}

/////////////////////////////////////////////////////////////

// QueryTopics 根据 Topic中的 Partition 偏移量, 查询Topic 分区实际的偏移量, 并替换到 Topic 中
//   - @param client: kafka 客户端
//   - @param topics: 需要查询的 Topic 列表
//   - - topic[partition] 的 offset:  OffsetNewest: 查询头部偏移量
//   - - topic[partition] 的 offset:  OffsetOldest: 查询尾部偏移量
//   - - topic[partition] 的 offset:  int64: 指定偏移量, 不进行查询
//   - @return []*Topic
func QueryTopics(client sarama.Client, topics ...*Topic) []*Topic {
	for _, topic := range topics {
		partitionList, err := client.Partitions(topic.Name)
		if err == nil {
			for _, partition := range partitionList {
				switch topic.Partition[partition] {
				case OffsetNewest:
					headOffset, _ := client.GetOffset(topic.Name, partition, OffsetNewest)
					topic.SetOffset(partition, headOffset)
				case OffsetOldest:
					tailOffset, _ := client.GetOffset(topic.Name, partition, OffsetOldest)
					topic.SetOffset(partition, tailOffset)
				default:

				}
			}
		}
	}

	return topics
}

/////////////////////////////////////////////////////////////

type NetConfig struct {
	MaxOpenRequests int           // 最大请求数
	DialTimeout     time.Duration // connect 超时时间
	ReadTimeout     time.Duration // read 超时时间
	WriteTimeout    time.Duration // write 超时时间
	ResolveHost     bool          // 是否使用域名, 使用集群域名时设置为true
}

func NewNetConfig() *NetConfig {
	return &NetConfig{
		MaxOpenRequests: 5,
		DialTimeout:     30000 * time.Millisecond,
		ReadTimeout:     120000 * time.Millisecond,
		WriteTimeout:    10000 * time.Millisecond,
		ResolveHost:     false,
	}
}

func (that *NetConfig) SetDialTimeout(dialTimeout time.Duration) *NetConfig {
	that.DialTimeout = dialTimeout
	return that
}

func (that *NetConfig) SetReadTimeout(readTimeout time.Duration) *NetConfig {
	that.ReadTimeout = readTimeout
	return that
}

func (that *NetConfig) SetWriteTimeout(writeTimeout time.Duration) *NetConfig {
	that.WriteTimeout = writeTimeout
	return that
}

func (that *NetConfig) SetMaxOpenRequests(maxOpenRequests int) *NetConfig {
	that.MaxOpenRequests = maxOpenRequests
	return that
}

func (that *NetConfig) SetResolveHost(resolveHost bool) *NetConfig {
	that.ResolveHost = resolveHost
	return that
}

/////////////////////////////////////////////////////////////

type ConsumerConfig struct {
	Min                int           // 每次从broker拉取的最小消息数
	Max                int           // 每次从broker拉取的最大消息数
	Fetch              int           // 每次从broker拉取的消息数
	InitialOffset      int64         // 消费者偏移量, -1: 从最新的消息开始消费, -2: 重新开始消费
	AutoCommit         bool          // 是否自动commit
	AutoCommitInterval time.Duration // 自动commit的情况下, 多久定时commit一次, 单位ms

	// 负载均衡策略, 可选范围[sticky|roundrobin|range], 默认为range,
	//    sticky: 粘性分配策略;
	//    roundrobin: 字典轮询分配策略;
	//    range: 范围分配策略
	Assignor string

	HeartbeatInterval time.Duration // 心跳间隔时间
	RebalanceTimeout  time.Duration // 重分配超时时间
	SessionTimeout    time.Duration // session超时时间
}

func NewKafkaConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		Min:                100,
		Max:                500,
		Fetch:              200,
		InitialOffset:      OffsetNewest,
		AutoCommit:         true,
		AutoCommitInterval: 5000 * time.Millisecond,
		Assignor:           "range",
		HeartbeatInterval:  5000 * time.Millisecond,
		RebalanceTimeout:   60000 * time.Millisecond,
		SessionTimeout:     10000 * time.Millisecond,
	}
}

func (that *ConsumerConfig) SetFetch(min int, max int, val int) *ConsumerConfig {
	that.Min = min
	that.Max = max
	that.Fetch = val
	return that
}

func (that *ConsumerConfig) SetInitialOffset(offset int64) *ConsumerConfig {
	that.InitialOffset = offset
	return that
}

func (that *ConsumerConfig) EnableAutoCommit(interval time.Duration) *ConsumerConfig {
	that.AutoCommit = true
	that.AutoCommitInterval = interval
	return that
}

func (that *ConsumerConfig) DisableAutoCommit() *ConsumerConfig {
	that.AutoCommit = false
	that.AutoCommitInterval = 1000 * time.Millisecond
	return that
}

func (that *ConsumerConfig) SetAssignor(assignor string) *ConsumerConfig {
	// 分区分配策略
	switch assignor {
	case "sticky", "roundrobin", "range":
		that.Assignor = assignor
	default:
		that.Assignor = "range"
	}
	return that
}

func (that *ConsumerConfig) GetAssignor() sarama.BalanceStrategy {
	switch that.Assignor {
	case "sticky":
		return sarama.NewBalanceStrategySticky()
	case "roundrobin":
		return sarama.NewBalanceStrategyRoundRobin()
	case "range":
		return sarama.NewBalanceStrategyRange()
	}
	return nil
}

func (that *ConsumerConfig) SetHeartbeatInterval(interval time.Duration) *ConsumerConfig {
	that.HeartbeatInterval = interval
	return that
}

func (that *ConsumerConfig) SetRebalanceTimeout(timeout time.Duration) *ConsumerConfig {
	that.RebalanceTimeout = timeout
	return that
}

func (that *ConsumerConfig) SetSessionTimeout(timeout time.Duration) *ConsumerConfig {
	that.SessionTimeout = timeout
	return that
}

/////////////////////////////////////////////////////////////

type ProducerConfig struct {
	Compression      string        // 压缩方式: 默认snappy , 可选[none, gzip, snappy, lz4, zstd]
	CompressionLevel int           // 压缩级别  默认 -1000
	MaxMessageBytes  int           // 每条消息最大字节, 默认100M
	RequiredAcks     string        // 发送确认: 默认 none, 可选[none, local, all]
	FlushMessages    int           // 刷新消息数量: 每100条刷新
	FlushFrequency   time.Duration // 缓存时间, 超过时长自动flush
	FlushMaxMessages int           // 最大刷新消息数量: 10000条
	RetryMax         int           // 重试次数: 最多重试3次
	Timeout          time.Duration // 超时时间: 10s
}

func NewKafkaProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		Compression:      "snappy",
		CompressionLevel: -1000,
		MaxMessageBytes:  100 * 1024 * 1024,
		RequiredAcks:     "none",
		FlushMessages:    100,
		FlushFrequency:   1000 * time.Millisecond,
		FlushMaxMessages: 10000,
		RetryMax:         3,
		Timeout:          10000 * time.Millisecond,
	}
}

func (that *ProducerConfig) SetCompression(compression string, level int) *ProducerConfig {
	switch compression {
	case "none", "gzip", "snappy", "lz4", "zstd":
		that.RequiredAcks = compression
		that.CompressionLevel = level
	default:
		that.RequiredAcks = "none"
		that.CompressionLevel = -1000
	}

	return that
}

func (that *ProducerConfig) GetCompression() sarama.CompressionCodec {
	switch that.Compression {
	case "none":
		return sarama.CompressionNone
	case "gzip":
		return sarama.CompressionGZIP
	case "snappy":
		return sarama.CompressionSnappy
	case "lz4":
		return sarama.CompressionLZ4
	case "zstd":
		return sarama.CompressionZSTD
	default:
		return sarama.CompressionNone
	}
}

func (that *ProducerConfig) SetMaxMessageBytes(limit int) *ProducerConfig {
	that.MaxMessageBytes = limit
	return that
}

func (that *ProducerConfig) SetRequiredAcks(acks string) *ProducerConfig {
	switch acks {
	case "none", "local", "all":
		that.RequiredAcks = acks
	default:
		that.RequiredAcks = "none"
	}
	return that
}

func (that *ProducerConfig) GetRequiredAcks() sarama.RequiredAcks {
	switch that.RequiredAcks {
	case "none":
		return sarama.NoResponse
	case "local":
		return sarama.WaitForLocal
	case "all":
		return sarama.WaitForAll
	default:
		return sarama.NoResponse
	}
}

func (that *ProducerConfig) SetFlush(limit, max int, frequency time.Duration) *ProducerConfig {
	that.FlushMessages = limit
	that.FlushFrequency = frequency
	that.FlushMaxMessages = max
	return that
}

func (that *ProducerConfig) SetRetry(max int) *ProducerConfig {
	that.RetryMax = max
	return that
}

func (that *ProducerConfig) SetTimeout(timeout time.Duration) *ProducerConfig {
	that.Timeout = timeout
	return that
}

/////////////////////////////////////////////////////////////

type Config struct {
	Version  sarama.KafkaVersion
	ClientId string
	GroupID  string

	Brokers *klists.KList[string] // 设置Broker

	Topics *klists.KList[*Topic] // 设置Topic

	ChannelBufferSize int             // 设置通道缓冲区大小
	Net               *NetConfig      // 设置网络配置
	Consumer          *ConsumerConfig // 设置消费配置
	Producer          *ProducerConfig // 设置生产配置

	OnError ErrorCallbackFunc // 设置错误回调
	OnExit  EventCallbackFunc // 设置退出回调
}

func NewKafkaConfig() *Config {
	return &Config{
		Version:           sarama.V0_10_2_0,
		ClientId:          "kafka_rpc",
		GroupID:           "kafka_rpc",
		Brokers:           klists.New[string](),
		Topics:            klists.New[*Topic](),
		ChannelBufferSize: 10000,
		Net:               NewNetConfig(),
		Consumer:          NewKafkaConsumerConfig(),
		Producer:          NewKafkaProducerConfig(),
	}
}

// 设置版本
//   - @param version string kafka 版本, 例如 "3.0.0"
func (that *Config) SetVersion(version string) *Config {
	ver, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		that.Version = sarama.V0_10_2_0
	} else {
		that.Version = ver
	}
	return that
}

func (that *Config) SetClientID(id string) *Config {
	that.ClientId = id
	return that
}
func (that *Config) SetGroupID(id string) *Config {
	that.GroupID = id
	return that
}

func (that *Config) AddBrokers(brokers ...string) *Config {
	for _, broker := range brokers {
		that.Brokers.PushBack(broker)
	}
	return that
}

func (that *Config) RemoveBrokers(brokers ...string) *Config {
	for _, broker := range brokers {
		that.Brokers.PopIf(func(item string) bool { return item == broker })
	}
	return that
}

func (that *Config) AddTopic(topics ...*Topic) *Config {
	for _, topic := range topics {
		that.Topics.PushBack(topic)
	}
	return that
}

func (that *Config) RemoveTopic(topics ...string) *Config {
	for _, topic := range topics {
		that.Topics.PopIf(func(item *Topic) bool { return item.Name == topic })
	}
	return that
}

func (that *Config) SetChannelBufferSize(size int) *Config {
	that.ChannelBufferSize = size
	return that
}

func (that *Config) SetNet(net *NetConfig) *Config {
	that.Net = net
	return that
}

func (that *Config) SetConsumer(consumer *ConsumerConfig) *Config {
	that.Consumer = consumer
	return that
}

func (that *Config) SetProducer(producer *ProducerConfig) *Config {
	that.Producer = producer
	return that
}

func (that *Config) SetErrorCallback(callback ErrorCallbackFunc) *Config {
	that.OnError = callback
	return that
}

func (that *Config) SetExitCallback(callback EventCallbackFunc) *Config {
	that.OnExit = callback
	return that
}
