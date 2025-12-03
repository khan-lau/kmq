package kafka

import (
	"slices"

	"github.com/IBM/sarama"
	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/container/klists"
	"github.com/khan-lau/kutils/container/kstrings"
	klog "github.com/khan-lau/kutils/klogger"
)

type SubscribeCallback func(voidObj interface{}, msg *KafkaMessage)

const (
	KAFKA_OFFSET_NEWEST = -1
	KAFKA_OFFSET_OLDEST = -2

	DEFAULT_OFFSET = KAFKA_OFFSET_NEWEST
)

/////////////////////////////////////////////////////////////

type Consumer struct {
	ctx        *kcontext.ContextNode
	conf       *Config
	Consumer   sarama.Consumer
	brokerList []string
	topics     []*Topic // 每个 topic 及其偏移量
	offset     int64
	logf       klog.AppLogFuncWithTag
}

// NewConsumer 使用提供的 broker 列表、日志函数和主题初始化一个新的 Kafka 消费者。
//   - @param ctx: 消费者的上下文。
//   - @param brokerList: broker 地址的列表。
//   - @param logf: 用于错误日志记录的日志函数。
//   - @param offset: 消费者的偏移量, 默认为 -1,
//   - - -1 : OffsetNewest; 从最新的消息开始消费
//   - - -2 : OffsetOldest; 重新开始消费
//   - @param topics: 要消费的 Kafka 主题。
//   - @return *Consumer: 指向新创建的 Consumer 实例的指针。
func NewConsumer(ctx *kcontext.ContextNode, conf *Config, logf klog.AppLogFuncWithTag) (*Consumer, error) {
	config := sarama.NewConfig()
	// 设置config
	config.Version = conf.Version                     // 设置协议版本
	config.ClientID = conf.ClientId                   // 设置客户端 ID
	config.ChannelBufferSize = conf.ChannelBufferSize // 设置通道缓冲区大小

	config.Consumer.Fetch.Min = int32(conf.Consumer.Min)                           // 每次从broker拉取的最小消息数
	config.Consumer.Fetch.Max = int32(conf.Consumer.Max)                           // 每次从broker拉取的最大消息数
	config.Consumer.Fetch.Default = int32(conf.Consumer.Fetch)                     // 默认每次从broker拉取的消息数
	config.Consumer.Offsets.Initial = conf.Consumer.InitialOffset                  // 设置消费者偏移量, -1: 从最新的消息开始消费, -2: 重新开始消费
	config.Consumer.Offsets.AutoCommit.Enable = conf.Consumer.AutoCommit           // 是否自动提交偏移量
	config.Consumer.Offsets.AutoCommit.Interval = conf.Consumer.AutoCommitInterval // 自动提交偏移量的间隔时间
	config.Consumer.Return.Errors = true                                           // 是否返回消费过程中遇到的错误, 默认为false

	// 网络配置
	config.Net.MaxOpenRequests = conf.Net.MaxOpenRequests // 最大请求数, 默认为5，这里设置为1，避免并发请求过多导致Kafka端出现问题
	config.Net.DialTimeout = conf.Net.DialTimeout         // 连接超时时间，默认为30秒
	config.Net.ReadTimeout = conf.Net.ReadTimeout         // 从连接读取消息的超时时间，默认为120秒
	config.Net.WriteTimeout = conf.Net.WriteTimeout       // 向连接写入消息的超时时间，默认为10秒

	// 这一行的作用是: 设置客户端是否在连接 Kafka 时尝试解析 Kafka 集群的主机名称, 默认为 true。
	// 如果设置为 true, 当 Kafka 集群的主机名称为 IP 地址时, 可能会导致连接失败, 因此这里设置为 false。
	config.Net.ResolveCanonicalBootstrapServers = conf.Net.ResolveHost

	brokerList := klists.ToKSlice(conf.Brokers)
	client, err := sarama.NewClient(brokerList, config)
	if err != nil {
		if logf != nil {
			logf(klog.ErrorLevel, kafka_tag, "kafka.NewClient error: {}", err.Error())
		}
		return nil, err
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	// consumer, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		if logf != nil {
			logf(klog.ErrorLevel, kafka_tag, "kafka.NewConsumer error: {}", err.Error())
		}
		return nil, err
	}

	topics := klists.ToKSlice(conf.Topics)
	topics = QueryTopics(client, topics...)

	subCtx := ctx.NewChild("kafka_single_consumer")
	return &Consumer{
		ctx:        subCtx,
		brokerList: brokerList,
		conf:       conf,
		Consumer:   consumer,
		topics:     topics,
		offset:     DEFAULT_OFFSET,
		logf:       logf,
	}, nil
}

func (that *Consumer) Subscribe(voidObj interface{}, callback SubscribeCallback) {
	go that.SyncSubscribe(voidObj, callback)
}

// Subscribe 订阅Kafka主题并异步处理消息。从最新偏移量开始收取消息。 该函数会阻塞, 直到 Close() 被调用。
//   - @param offset: 消费者的偏移量
//   - @param callback: 当收到新消息时要调用的函数。
func (that *Consumer) SyncSubscribe(voidObj interface{}, callback SubscribeCallback) {
	type SubContext struct {
		topic     string
		partition int32
		ctx       *kcontext.ContextNode
		pc        sarama.PartitionConsumer
	}
	contextList := klists.New[*SubContext]()
	for _, topic := range that.topics {
		partitionList, err := that.Consumer.Partitions(topic.Name)
		if err != nil {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, kafka_tag, "Get partition list from kafka error: {}", err.Error())
			}
			return
		}
		for partition, offset := range topic.Partition {
			if !slices.Contains(partitionList, partition) {
				if that.logf != nil {
					that.logf(klog.ErrorLevel, kafka_tag, "Partition {} not found in topic {}", partition, topic)
				}
				continue
			}

			pc, err := that.Consumer.ConsumePartition(topic.Name, partition, offset)
			if err != nil {
				if that.logf != nil {
					that.logf(klog.ErrorLevel, kafka_tag, "Create topic {} partition {} consumer offset {} error: {}", topic.Name, partition, offset, err.Error())
				}

				// 如果初始偏移量不正确，则重新创建消费者并从配置文件的InitialOffset开始消费
				pc, err = that.Consumer.ConsumePartition(topic.Name, partition, that.conf.Consumer.InitialOffset)
				if err != nil {
					if that.logf != nil {
						that.logf(klog.ErrorLevel, kafka_tag, "Create topic {} partition {} consumer offset {} error: {}", topic.Name, partition, offset, err.Error())
					}
					continue
				}
			}

			subCtx := that.ctx.NewChild(kstrings.Sprintf("kafka_single_consumer_child_{}_{}", topic, partition))
			contextList.PushBack(&SubContext{topic: topic.Name, partition: partition, ctx: subCtx, pc: pc})

			go func(ctx *kcontext.ContextNode, pc sarama.PartitionConsumer, partition int32) {
			SUB_END_LOOP:
				for {
					select {
					case <-ctx.Context().Done():
						pc.Close()
						break SUB_END_LOOP

					case err := <-pc.Errors():
						if that.logf != nil {
							that.logf(klog.ErrorLevel, kafka_tag, "kafka.Consumer consume partition {} error: {}", partition, err.Error())
						}
						continue SUB_END_LOOP

					case msg := <-pc.Messages():
						if callback != nil {
							callback(voidObj, &KafkaMessage{Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset, Key: msg.Key, Value: msg.Value})
						}
					}
				}
				defer pc.Close()
			}(subCtx, pc, partition)

			if that.logf != nil {
				that.logf(klog.InfoLevel, kafka_tag, "kafka.Consumer subscribe topic: {}, partition: {} success", topic.Name, partition)
			}

		}

		// for _, partition := range partitionList {
		// 	offset := topic.Partition[partition]
		// 	if offset < 0 {
		// 		offset = that.offset
		// 	}
		// }
	}

	<-that.ctx.Context().Done()

	for it := contextList.Front(); it != nil; it = it.Next() {
		subContext := it.Value
		subContext.ctx.Cancel()
		subContext.pc.Close()
		subContext.ctx.Remove()
		if that.logf != nil {
			that.logf(klog.DebugLevel, kafka_tag, "kafka.Consumer unsubscribe topic: {}, partition: {} success", subContext.topic, subContext.partition)
		}
	}
}

func (that *Consumer) Close() {
	that.ctx.Cancel()
	that.Consumer.Close()
	that.ctx.Remove()
}

// func (that *Consumer) log(lvl klog.Level, f string, args ...interface{}) {
// 	if that.logf != nil {
// 		that.logf(lvl, f, args...)
// 	}
// }

/////////////////////////////////////////////////////////////

type ConsumerGroup struct {
	ctx        *kcontext.ContextNode
	conf       *Config
	group      sarama.ConsumerGroup
	brokerList []string
	topics     []*Topic // 每个 topic 及其偏移量
	logf       klog.AppLogFuncWithTag
}

func NewConsumerGroup(ctx *kcontext.ContextNode, conf *Config, logf klog.AppLogFuncWithTag) (*ConsumerGroup, error) {
	config := sarama.NewConfig()
	// 设置config
	config.Version = conf.Version                     // 设置协议版本
	config.ClientID = conf.ClientId                   // 设置客户端 ID
	config.ChannelBufferSize = conf.ChannelBufferSize // 设置通道缓冲区大小

	config.Consumer.Fetch.Min = int32(conf.Consumer.Min)                           // 每次从broker拉取的最小消息数
	config.Consumer.Fetch.Max = int32(conf.Consumer.Max)                           // 每次从broker拉取的最大消息数
	config.Consumer.Fetch.Default = int32(conf.Consumer.Fetch)                     // 默认每次从broker拉取的消息数
	config.Consumer.Offsets.Initial = conf.Consumer.InitialOffset                  // 设置消费者偏移量, -1: 从最新的消息开始消费, -2: 重新开始消费
	config.Consumer.Offsets.AutoCommit.Enable = conf.Consumer.AutoCommit           // 是否自动提交偏移量
	config.Consumer.Offsets.AutoCommit.Interval = conf.Consumer.AutoCommitInterval // 自动提交偏移量的间隔时间
	config.Consumer.Return.Errors = true                                           // 是否返回消费过程中遇到的错误, 默认为false

	// 分区分配策略
	assignor := conf.Consumer.GetAssignor()
	if assignor == nil {
		assignor = sarama.NewBalanceStrategyRange() // 默认采用 range 分配策略
	}
	config.Consumer.Group.Rebalance.Strategy = assignor

	config.Consumer.Group.Rebalance.Timeout = conf.Consumer.RebalanceTimeout   // 重分配超时时间
	config.Consumer.Group.Heartbeat.Interval = conf.Consumer.HeartbeatInterval // 心跳间隔时间
	config.Consumer.Group.Session.Timeout = conf.Consumer.SessionTimeout       // 会话过期时间

	// 网络配置
	config.Net.MaxOpenRequests = conf.Net.MaxOpenRequests // 最大请求数, 默认为5，这里设置为1，避免并发请求过多导致Kafka端出现问题
	config.Net.DialTimeout = conf.Net.DialTimeout         // 连接超时时间，默认为30秒
	config.Net.ReadTimeout = conf.Net.ReadTimeout         // 从连接读取消息的超时时间，默认为120秒
	config.Net.WriteTimeout = conf.Net.WriteTimeout       // 向连接写入消息的超时时间，默认为10秒

	// 这一行的作用是: 设置客户端是否在连接 Kafka 时尝试解析 Kafka 集群的主机名称, 默认为 true。
	// 如果设置为 true, 当 Kafka 集群的主机名称为 IP 地址时, 可能会导致连接失败, 因此这里设置为 false。
	config.Net.ResolveCanonicalBootstrapServers = conf.Net.ResolveHost

	brokerList := klists.ToKSlice(conf.Brokers)
	client, err := sarama.NewClient(brokerList, config)
	if err != nil {
		if logf != nil {
			logf(klog.ErrorLevel, kafka_tag, "kafka.NewClient error: {}", err.Error())
		}
		return nil, err
	}

	group, err := sarama.NewConsumerGroupFromClient(conf.GroupID, client)
	// group, err := sarama.NewConsumerGroup(brokerList, groupId, config)
	if err != nil {
		if logf != nil {
			logf(klog.ErrorLevel, kafka_tag, "kafka.NewConsumerGroup error: {}", err.Error())
		}
		return nil, err
	}

	topics := klists.ToKSlice(conf.Topics)
	topics = QueryTopics(client, topics...)

	subCtx := ctx.NewChild("kafka_group_consumer")
	return &ConsumerGroup{
		ctx:        subCtx,
		conf:       conf,
		group:      group,
		brokerList: brokerList,
		topics:     topics,
		logf:       logf,
	}, nil
}

func (that *ConsumerGroup) Subscribe(voidObj interface{}, callback SubscribeCallback) {
	go that.SyncSubscribe(voidObj, callback)
}

// Subscribe 订阅Kafka主题并异步处理消息。从最新偏移量开始收取消息。该函数会阻塞, 直到 Close() 被调用。
//   - @param callback: 当收到新消息时要调用的函数。
func (that *ConsumerGroup) SyncSubscribe(voidObj interface{}, callback SubscribeCallback) {
	msgChan := make(chan *KafkaMessage, 10000)

	tmpCtx := that.ctx.NewChild("kafka_group_consumer_tmp")

	topicNameList := make([]string, 0, len(that.topics))
	for _, topic := range that.topics {
		topicNameList = append(topicNameList, topic.Name)
	}

	//topics := maps.Keys(that.topics)

	consumerErrChan := make(chan error)
	// 定义消费者组处理程序
	handler := &privateConsumerGroupHandler{msgChan: msgChan, topics: that.topics, autoCommit: that.conf.Consumer.AutoCommit}
	go func(ctx *kcontext.ContextNode, topics []string, handler *privateConsumerGroupHandler) {
		for {
			if err := that.group.Consume(that.ctx.Context(), topics, handler); err != nil {
				if that.logf != nil {
					that.logf(klog.ErrorLevel, kafka_tag, "kafka.ConsumerGroup error: {}", err.Error())
				}
			}

			// 检查上下文是否被取消，如果是，函数传入的上下文被取消
			if ctx.Context().Err() != nil {
				consumerErrChan <- ctx.Context().Err()
				break
			}
		}
	}(tmpCtx, topicNameList, handler)

	subCtx := that.ctx.NewChild("kafka_group_consumer_child")
	go func(ctx *kcontext.ContextNode, msgChan chan *KafkaMessage) {
	END_LOOP:
		for {
			select {
			case <-ctx.Context().Done():
				break END_LOOP
			case <-consumerErrChan:
				break END_LOOP
			case msg := <-msgChan:
				if callback != nil {
					callback(voidObj, msg)
				}
			}
		}
	}(subCtx, msgChan)

	<-that.ctx.Context().Done()
	tmpCtx.Cancel()
	subCtx.Cancel()

	tmpCtx.Remove()
	subCtx.Remove()

	if err := that.group.Close(); err != nil {
		if that.logf != nil {
			that.logf(klog.ErrorLevel, kafka_tag, "Error closing client: {}", err.Error())
		}
	}

	if that.conf.OnExit != nil {
		that.conf.OnExit(nil)
	}
}

func (that *ConsumerGroup) Close() {
	that.ctx.Cancel()
	that.group.Close()
}

/////////////////////////////////////////////////////////////

// ConsumerGroupHandler 实例用于处理单个 topic/partition 的声明。
// 它还提供了用于处理消费者组会话生命周期的钩子，并允许在消费循环(s)之前或之后触发逻辑。
//
// 请注意，处理程序很可能从多个 goroutine 同时并发调用，
// 确保所有状态都安全地防止竞争条件。
type privateConsumerGroupHandler struct {
	msgChan    chan *KafkaMessage
	autoCommit bool
	topics     []*Topic
}

// Setup 是在新的会话开始之前调用的，在 ConsumeClaim 之前。
func (that *privateConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	for _, topic := range that.topics {
		for partition, offset := range topic.Partition {
			if offset >= 0 {
				session.ResetOffset(topic.Name, partition, offset, "")
				session.MarkOffset(topic.Name, partition, offset, "")
			}
		}
	}
	return nil
}

// Cleanup 是在会话结束之前运行的，在所有 ConsumeClaim 协程退出之后，
// 但在最后一次提交偏移量之前。
func (that *privateConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim 必须启动 ConsumerGroupClaim 的 Messages() 消费循环。
// 一旦 Messages() 通道被关闭，Handler 必须完成其消息处理循环并退出。
func (that *privateConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		that.msgChan <- &KafkaMessage{Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset, Key: msg.Key, Value: msg.Value, session: session}
		if that.autoCommit {
			session.MarkMessage(msg, "")
		}
	}
	return nil
}
