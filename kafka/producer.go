package kafka

import (
	"github.com/IBM/sarama"
	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/container/klists"
	klog "github.com/khan-lau/kutils/klogger"
)

// Producer 发送消息到 kafka
type SyncProducer struct {
	ctx        *kcontext.ContextNode
	brokerList []string
	conf       *Config
	Producer   sarama.SyncProducer
	msgChan    chan *KafkaMessage
	chanSize   uint // 消息通道大小
	logf       klog.AppLogFuncWithTag
}

// NewSyncProducer 初始化一个新的同步Kafka生产者。
//
// 参数:
//
//	ctx *kcontext.ContextNode - 用于生产者的上下文。
//	conf *Config - 配置信息。
//	logf klog.AppLogFunc - 用于错误处理的日志记录函数。
//
// 返回:
//
//	*SyncProducer - 指向初始化的SyncProducer的指针。
func NewSyncProducer(ctx *kcontext.ContextNode, chanSize uint, conf *Config, logf klog.AppLogFuncWithTag) (*SyncProducer, error) {
	config := sarama.NewConfig()
	// 设置config
	config.Version = conf.Version                     // 设置协议版本
	config.ClientID = conf.ClientId                   // 设置客户端 ID
	config.ChannelBufferSize = conf.ChannelBufferSize // 设置通道缓冲区大小

	// 网络配置
	config.Net.MaxOpenRequests = conf.Net.MaxOpenRequests // 最大请求数, 默认为5，这里设置为1，避免并发请求过多导致Kafka端出现问题
	config.Net.DialTimeout = conf.Net.DialTimeout         // 连接超时时间，默认为30秒
	config.Net.ReadTimeout = conf.Net.ReadTimeout         // 从连接读取消息的超时时间，默认为120秒
	config.Net.WriteTimeout = conf.Net.WriteTimeout       // 向连接写入消息的超时时间，默认为10秒

	// 这一行的作用是: 设置客户端是否在连接 Kafka 时尝试解析 Kafka 集群的主机名称, 默认为 false
	// 如果设置为 true, 当 Kafka 集群的主机名称为 IP 地址时, 可能会导致连接失败, 因此这里设置为 false。
	config.Net.ResolveCanonicalBootstrapServers = conf.Net.ResolveHost

	// 设置 Producer 的配置
	config.Producer.Compression = conf.Producer.GetCompression()       // 设置压缩方式: snappy
	config.Producer.CompressionLevel = conf.Producer.CompressionLevel  // 设置压缩级别
	config.Producer.MaxMessageBytes = conf.Producer.MaxMessageBytes    // 发送限制
	config.Producer.RequiredAcks = conf.Producer.GetRequiredAcks()     // 设置确认模式: 本地文件写入成功，并不代表已经通知服务器
	config.Producer.Return.Errors = true                               //是否返回消费过程中遇到的错误, 默认为false
	config.Producer.Return.Successes = true                            //是否返回消费完成, 默认为false
	config.Producer.Flush.Messages = conf.Producer.FlushMessages       // 设置刷新消息数量: 每100条刷新
	config.Producer.Flush.Frequency = conf.Producer.FlushFrequency     // 缓存时间
	config.Producer.Flush.MaxMessages = conf.Producer.FlushMaxMessages // 设置刷新最大消息数量: 10000条
	config.Producer.Retry.Max = conf.Producer.RetryMax                 // 设置重试次数: 最多重试3次
	config.Producer.Timeout = conf.Producer.Timeout                    // 设置发送超时时间: 30s

	brokerList := klists.ToKSlice(conf.Brokers)
	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		if logf != nil {
			logf(klog.ErrorLevel, kafka_tag, "kafka.NewSyncProducer error: {}", err.Error())
		}
		return nil, err
	}

	msgChan := make(chan *KafkaMessage, chanSize) // 初始化消息通道

	subCtx := ctx.NewChild("kafka_sync_producer")
	producerClient := &SyncProducer{
		ctx:        subCtx,
		brokerList: brokerList,
		conf:       conf,
		Producer:   producer,
		msgChan:    msgChan,
		chanSize:   chanSize,
		logf:       logf,
	}

	if conf != nil && conf.OnReady != nil {
		conf.OnReady(true)
	}
	return producerClient, nil
}

func (that *SyncProducer) Start() {
	go that.SyncStart()
}

func (that *SyncProducer) SyncStart() {
END_LOOP:
	for {
		select {
		case <-that.ctx.Context().Done():
			// that.log(klog.InfoLevel, "kafka.SyncProducer cancel")
			break END_LOOP
		case msg := <-that.msgChan:
			// that.log(klog.DebugLevel, "ready to topic: {} send {}", msg.Topic, string(msg.Value))
			rawMsg := &sarama.ProducerMessage{
				Topic:   msg.Topic,
				Value:   sarama.ByteEncoder(msg.Value),
				Headers: msg.Headers,
			}

			// 发送消息
			partition, offset, err := that.Producer.SendMessage(rawMsg)
			if err != nil {
				if that.logf != nil {
					that.logf(klog.ErrorLevel, kafka_tag, "kafka.SendMessage error: {}", err.Error())
				}
				return
			} else {
				if that.logf != nil {
					that.logf(klog.DebugLevel, kafka_tag, "send to topic: {} message {} partition: {} offset: {}", msg.Topic, string(msg.Value), partition, offset)
				}
			}
		}
	}

	<-that.ctx.Context().Done()

	if that.logf != nil {
		that.logf(klog.InfoLevel, kafka_tag, "kafka.SyncProducer close")
	}

	if that.conf.OnExit != nil {
		that.conf.OnExit(nil)
	}
}

func (that *SyncProducer) Publish(msg *KafkaMessage) bool {
	if that != nil && that.msgChan != nil {
		select {
		case that.msgChan <- msg:
			return true
		default:
		}
	}
	return false
}

func (that *SyncProducer) PublisMessage(topic string, key, message string) bool {
	msg := &KafkaMessage{
		Topic:     topic,
		Partition: 0,
		Offset:    0,
		Key:       []byte(key),
		Value:     []byte(message),
	}
	return that.Publish(msg)
}

func (that *SyncProducer) PublisData(topic string, key string, value []byte) bool {
	msg := &KafkaMessage{
		Topic:     topic,
		Partition: 0,
		Offset:    0,
		Key:       []byte(key),
		Value:     value,
	}
	return that.Publish(msg)
}

func (that *SyncProducer) PublisDataWithProperties(partition int32, topic, key string, value []byte, properties map[string]string) bool {
	headers := make([]sarama.RecordHeader, 0, len(properties))
	for k, v := range properties {
		headers = append(headers, sarama.RecordHeader{Key: []byte(k), Value: []byte(v)})
	}
	msg := &KafkaMessage{
		Topic:     topic,
		Partition: partition,
		Offset:    0,
		Key:       []byte(key),
		Value:     value,
		Headers:   headers,
	}
	return that.Publish(msg)
}

func (that *SyncProducer) Close() {
	that.ctx.Cancel()
	that.Producer.Close()
	that.ctx.Remove()
}

/////////////////////////////////////////////////////////////

type AsyncProducer struct {
	ctx        *kcontext.ContextNode
	brokerList []string
	conf       *Config
	Producer   sarama.AsyncProducer
	msgChan    chan *KafkaMessage
	chanSize   uint // 消息通道大小
	logf       klog.AppLogFuncWithTag
}

// NewAsyncProducer 创建一个新的异步 Kafka 生产者。
//
// 参数:
//
//	ctx：生产者的上下文。
//	brokerList：连接到 Kafka 的 broker 列表。
//	logf：用于记录错误的日志函数。
//
// 返回:
//
//	*AsyncProducer：创建的异步生产者的指针。
func NewAsyncProducer(ctx *kcontext.ContextNode, chanSize uint, conf *Config, logf klog.AppLogFuncWithTag) (*AsyncProducer, error) {
	config := sarama.NewConfig()
	// 设置config
	config.Version = conf.Version                     // 设置协议版本
	config.ClientID = conf.ClientId                   // 设置客户端 ID
	config.ChannelBufferSize = conf.ChannelBufferSize // 设置通道缓冲区大小

	// 网络配置
	config.Net.MaxOpenRequests = conf.Net.MaxOpenRequests // 最大请求数, 默认为5，这里设置为1，避免并发请求过多导致Kafka端出现问题
	config.Net.DialTimeout = conf.Net.DialTimeout         // 连接超时时间，默认为30秒
	config.Net.ReadTimeout = conf.Net.ReadTimeout         // 从连接读取消息的超时时间，默认为120秒
	config.Net.WriteTimeout = conf.Net.WriteTimeout       // 向连接写入消息的超时时间，默认为10秒

	// 这一行的作用是: 设置客户端是否在连接 Kafka 时尝试解析 Kafka 集群的主机名称, 默认为 false
	// 如果设置为 true, 当 Kafka 集群的主机名称为 IP 地址时, 可能会导致连接失败, 因此这里设置为 false。
	config.Net.ResolveCanonicalBootstrapServers = conf.Net.ResolveHost

	// 设置 Producer 的配置
	config.Producer.Compression = conf.Producer.GetCompression()       // 设置压缩方式: snappy
	config.Producer.CompressionLevel = conf.Producer.CompressionLevel  // 设置压缩级别
	config.Producer.MaxMessageBytes = conf.Producer.MaxMessageBytes    // 发送限制
	config.Producer.RequiredAcks = conf.Producer.GetRequiredAcks()     // 设置确认模式: 本地文件写入成功，并不代表已经通知服务器
	config.Producer.Return.Errors = true                               // 是否返回消费过程中遇到的错误, 默认为false
	config.Producer.Return.Successes = true                            // 是否返回消费完成, 默认为false
	config.Producer.Flush.Messages = conf.Producer.FlushMessages       // 设置刷新消息数量: 每100条刷新
	config.Producer.Flush.Frequency = conf.Producer.FlushFrequency     // 缓存时间
	config.Producer.Flush.MaxMessages = conf.Producer.FlushMaxMessages // 设置刷新最大消息数量: 10000条
	config.Producer.Retry.Max = conf.Producer.RetryMax                 // 设置重试次数: 最多重试3次
	config.Producer.Timeout = conf.Producer.Timeout                    // 设置发送超时时间: 30s

	brokerList := klists.ToKSlice(conf.Brokers)
	producer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		if logf != nil {
			logf(klog.ErrorLevel, kafka_tag, "kafka.NewAsyncProducer error: {}", err.Error())
		}
		return nil, err
	}

	msgChan := make(chan *KafkaMessage, chanSize)

	subCtx := ctx.NewChild("kafka_async_producer")
	producerClient := &AsyncProducer{
		ctx:        subCtx,
		brokerList: brokerList,
		conf:       conf,
		Producer:   producer,
		msgChan:    msgChan,
		chanSize:   chanSize,
		logf:       logf,
	}
	return producerClient, nil
}

func (that *AsyncProducer) Start() {

	tmpCtx := that.ctx.NewChild("kafka_single_producer_child")
	go func(ctx *kcontext.ContextNode) {
	END_LOOP:
		for {
			select {
			case <-ctx.Context().Done():
				break END_LOOP
			case success := <-that.Producer.Successes():
				byteArr, _ := success.Value.Encode()
				if that.logf != nil {
					that.logf(klog.DebugLevel, kafka_tag, "Message sent to Kafka topic {}, partition {}, offset {} msg: {}", success.Topic, success.Partition, success.Offset, string(byteArr))
				}
			case err := <-that.Producer.Errors():
				if that.logf != nil {
					that.logf(klog.ErrorLevel, kafka_tag, "Failed to send message to Kafka topic {}, partition {}: {}", err.Msg.Topic, err.Msg.Partition, err.Err.Error())
				}
				if that.conf.OnError != nil {
					that.conf.OnError(err)
				}
			}
		}
	}(tmpCtx)

	daemonCtx := that.ctx.NewChild("kafka_single_producer_daemon_child")
	go func(ctx *kcontext.ContextNode) {
	END_LOOP:
		for {
			select {
			case <-ctx.Context().Done():
				break END_LOOP
			case msg := <-that.msgChan:
				// that.logf(klog.InfoLevel, kafka_tag, "kafka.AsyncProducer publish key:{} message: {}", string(msg.Key), string(msg.Value))
				rawMsg := &sarama.ProducerMessage{Topic: msg.Topic, Key: sarama.StringEncoder(msg.Key), Value: sarama.ByteEncoder(msg.Value), Headers: msg.Headers}
				that.Producer.Input() <- rawMsg
			}
		}
	}(daemonCtx)

	if that.conf != nil && that.conf.OnReady != nil {
		that.conf.OnReady(true)
	}

	<-that.ctx.Context().Done()
	tmpCtx.Cancel()
	tmpCtx.Remove()

	if that.conf.OnExit != nil {
		that.conf.OnExit(nil)
	}
}

func (that *AsyncProducer) Publish(msg *KafkaMessage) bool {
	if that != nil && that.msgChan != nil {
		select {
		case that.msgChan <- msg:
			return true
		default:
		}
	}
	return false
}

func (that *AsyncProducer) PublisMessage(topic, key, message string) bool {
	msg := &KafkaMessage{
		Topic:     topic,
		Partition: 0,
		Offset:    0,
		Key:       []byte(key),
		Value:     []byte(message),
	}
	return that.Publish(msg)
}

func (that *AsyncProducer) PublisData(partition int32, topic, key string, value []byte) bool {
	msg := &KafkaMessage{
		Topic:     topic,
		Partition: partition,
		Offset:    0,
		Key:       []byte(key),
		Value:     value,
	}
	return that.Publish(msg)
}

func (that *AsyncProducer) PublisDataWithProperties(partition int32, topic, key string, value []byte, properties map[string]string) bool {
	headers := make([]sarama.RecordHeader, 0, len(properties))
	for k, v := range properties {
		headers = append(headers, sarama.RecordHeader{Key: []byte(k), Value: []byte(v)})
	}
	msg := &KafkaMessage{
		Topic:     topic,
		Partition: partition,
		Offset:    0,
		Key:       []byte(key),
		Value:     value,
		Headers:   headers,
	}
	return that.Publish(msg)
}

func (that *AsyncProducer) Close() {
	that.ctx.Cancel()
	that.ctx.Remove()
	that.Producer.AsyncClose()
}
