package target

import (
	"time"

	"github.com/khan-lau/kmq/example/bean/config"
	"github.com/khan-lau/kmq/example/service/idl"
	"github.com/khan-lau/kmq/kafka"

	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/container/kstrings"
	klog "github.com/khan-lau/kutils/klogger"
)

type KafkaMQ struct {
	ctx       *kcontext.ContextNode
	conf      *config.KafkaConfig
	name      string // 服务名称
	status    idl.ServiceStatus
	chanSize  uint
	publisher *kafka.AsyncProducer
	onReady   kafka.ReadyCallbackFunc
	logf      klog.AppLogFuncWithTag
}

const (
	kafkamq_tag = "kafkamq_target"
)

func NewKafkaMQ(ctx *kcontext.ContextNode, name string, conf *config.KafkaConfig, chanSize uint, logf klog.AppLogFuncWithTag) (*KafkaMQ, error) {
	subCtx := ctx.NewChild(kstrings.FormatString("{}_{}", kafkamq_tag, name))

	rabbitMQ := &KafkaMQ{
		ctx:       subCtx,
		conf:      conf,
		name:      name,
		status:    idl.ServiceStatusStopped,
		chanSize:  chanSize,
		publisher: nil,
		logf:      logf,
	}
	_ = rabbitMQ.SetOnReady(func(ready bool) {
		if rabbitMQ.onReady != nil {
			rabbitMQ.onReady(ready)
		}
	})
	return rabbitMQ, nil
}

func (that *KafkaMQ) Name() string {
	return that.name
}

func (that *KafkaMQ) Init() {}

func (that *KafkaMQ) StartAsync() {
	go func() {
		err := that.Start()
		if err != nil {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, kafkamq_tag, "start service {} error: {}", that.name, err)
			}
			that.onError(that.name, err)
		}
	}()
}

func (that *KafkaMQ) Start() error {
	if that.status != idl.ServiceStatusStopped { //检查服务状态 是否为停止状态
		return kstrings.Errorf("service {} is not stopped, status={}", that.name, that.status)
	}

	netConfig := kafka.NewNetConfig().
		SetDialTimeout(time.Duration(that.conf.Net.DialTimeout) * time.Millisecond).
		SetMaxOpenRequests(that.conf.Net.MaxOpenRequests).
		SetReadTimeout(time.Duration(that.conf.Net.ReadTimeout) * time.Millisecond).
		SetWriteTimeout(time.Duration(that.conf.Net.WriteTimeout) * time.Millisecond).
		SetResolveHost(that.conf.Net.ResolveHost)

	kafkaProducerConfig := kafka.NewKafkaProducerConfig().
		SetCompression(that.conf.Producer.Compression, that.conf.Producer.CompressionLevel).
		SetMaxMessageBytes(that.conf.Producer.MaxMessageBytes).
		SetRequiredAcks(that.conf.Producer.RequiredAcks).
		SetFlush(that.conf.Producer.FlushMessages, that.conf.Producer.FlushFrequency, time.Duration(that.conf.Producer.FlushMaxMessages)*time.Millisecond).
		SetRetry(that.conf.Producer.RetryMax).
		SetTimeout(time.Duration(that.conf.Producer.Timeout) * time.Millisecond)

	// 设置topic
	topics := make([]*kafka.Topic, 0, len(that.conf.Producer.Topics))
	for _, item := range that.conf.Producer.Topics {
		topic := kafka.NewTopic(item.Name)
		for _, partition := range item.Partitions {
			topic.SetOffset(int32(partition.Partition), partition.Offset)
		}
		topics = append(topics, topic)
	}

	kafkaConfig := kafka.NewKafkaConfig().
		SetVersion(that.conf.Version).
		SetClientID(that.conf.ClientID).
		SetGroupID(that.conf.GroupID).
		AddBrokers(that.conf.BrokerList...).
		AddTopic(topics...).
		SetChannelBufferSize(that.conf.ChannelBufferSize).
		SetNet(netConfig).
		SetProducer(kafkaProducerConfig)

	publisher, err := kafka.NewAsyncProducer(that.ctx, uint(that.chanSize), kafkaConfig, that.logf)
	if err != nil {
		return err
	}

	kafkaConfig.SetExitCallback(func(event interface{}) {
		that.onExit(event)
	})

	kafkaConfig.SetErrorCallback(func(err error) {
		that.onError(that.name, err)
	})
	that.publisher = publisher
	go func() {
		// sleep 500ms, 等待服务启动完成
		time.Sleep(500 * time.Millisecond)
		that.status = idl.ServiceStatusRunning //设置服务状态为运行状态
	}()

	that.publisher.Start()

	return nil
}

func (that *KafkaMQ) Restart() error {
	if that.status != idl.ServiceStatusRunning { //检查服务状态 是否为运行状态
		err := that.Stop()
		if err != nil {
			return err
		}
	}

	err := that.Start()
	return err
}

func (that *KafkaMQ) Stop() error {
	that.ctx.Cancel()
	if that.publisher != nil {
		that.publisher.Close()
	}
	that.status = idl.ServiceStatusStopped // 设置服务状态为停止状态
	that.publisher = nil
	time.Sleep(500 * time.Millisecond)
	that.ctx.Remove()
	return nil
}

func (that *KafkaMQ) Broadcast(message []byte, properties map[string]string) bool {
	for _, topic := range that.conf.Producer.Topics {
		key := ""
		if properties != nil {
			key = properties["key"]
		}
		// if !that.PublishMessageWithProperties(int32(topic.Partition), topic.Name, key, message, properties) {
		// 	if that.logf != nil {
		// 		that.logf(klog.ErrorLevel, kafkamq_tag, "publish topic {} partition {} message {} fault", topic.Name, topic.Partition, string(message))
		// 	}
		// }

		if !that.PublishMessageWithProperties(int32(0), topic.Name, key, message, properties) {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, kafkamq_tag, "publish topic {} message {} fault", topic.Name, string(message))
			}
		}
	}
	return true
}

func (that *KafkaMQ) Publish(topic string, message []byte, properties map[string]string) bool {
	key := ""
	if properties != nil {
		key = properties["key"]
	}
	// that.logf(klog.DebugLevel, kafkamq_tag, "publish topic {}, key {}, message {}", topic, key, string(message))
	return that.PublishMessageWithProperties(0, topic, key, message, properties)
}

func (that *KafkaMQ) PublishMessage(partition int32, topic, key string, value []byte) bool {
	if that.status != idl.ServiceStatusRunning { //检查服务状态 是否为运行状态
		return false
	}

	// that.publisher
	return that.publisher.PublisDataWithProperties(partition, topic, key, value, nil)
}

// PublishMessageWithProperties 带属性的发布消息
// 参数:
//
//	partition - 分区号 该参数暂时无效
//	topic     - 主题名
//	key       - 消息键值
//	value     - 消息内容
//	properties- 属性列表
func (that *KafkaMQ) PublishMessageWithProperties(partition int32, topic, key string, value []byte, properties map[string]string) bool {
	if that.status != idl.ServiceStatusRunning { //检查服务状态 是否为运行状态
		return false
	}
	return that.publisher.PublisDataWithProperties(partition, topic, key, value, properties)
}

func (that *KafkaMQ) onError(obj interface{}, err error) {
}

func (that *KafkaMQ) onExit(obj interface{}) {
}
func (that *KafkaMQ) SetOnReady(callback kafka.ReadyCallbackFunc) *KafkaMQ {
	that.onReady = callback
	return that
}
