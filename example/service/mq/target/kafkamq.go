package target

import (
	"context"
	"time"

	"github.com/khan-lau/kmq/example/bean/config"
	"github.com/khan-lau/kmq/example/service/idl"
	"github.com/khan-lau/kmq/kafka"

	"github.com/khan-lau/kutils/container/kstrings"
	"github.com/khan-lau/kutils/logger"
)

type KafkaMQ struct {
	ctx       context.Context
	cancel    context.CancelFunc
	conf      *config.KafkaConfig
	name      string // 服务名称
	status    idl.ServiceStatus
	publisher *kafka.AsyncProducer

	logf logger.AppLogFuncWithTag
}

const (
	kafkamq_tag = "kafkamq_target"
)

func NewKafkaMQ(ctx context.Context, name string, conf *config.KafkaConfig, logf logger.AppLogFuncWithTag) (*KafkaMQ, error) {
	subCtx, subCancel := context.WithCancel(ctx)

	rabbitMQ := &KafkaMQ{
		ctx:       subCtx,
		cancel:    subCancel,
		conf:      conf,
		name:      name,
		status:    idl.ServiceStatusStopped,
		publisher: nil,
		logf:      logf,
	}

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
				that.logf(logger.ErrorLevel, kafkamq_tag, "start service {} error: {}", that.name, err)
			}
			that.onError(that.name, err)
		}
	}()
}

func (that *KafkaMQ) Start() error {
	if that.status != idl.ServiceStatusStopped { //检查服务状态 是否为停止状态
		return kstrings.Errorf("service {} is not stopped, status={}", that.name, that.status)
	}

	subCtx := context.WithoutCancel(that.ctx)

	netConfig := kafka.NewNetConfig().
		SetDialTimeout(time.Duration(that.conf.Net.DialTimeout)).
		SetMaxOpenRequests(that.conf.Net.MaxOpenRequests).
		SetReadTimeout(time.Duration(that.conf.Net.ReadTimeout)).
		SetWriteTimeout(time.Duration(that.conf.Net.WriteTimeout)).
		SetResolveHost(that.conf.Net.ResolveHost)

	kafkaProducerConfig := kafka.NewKafkaProducerConfig().
		SetCompression(that.conf.Producer.Compression, that.conf.Producer.CompressionLevel).
		SetMaxMessageBytes(that.conf.Producer.MaxMessageBytes).
		SetRequiredAcks(that.conf.Producer.RequiredAcks).
		SetFlush(that.conf.Producer.FlushMessages, that.conf.Producer.FlushFrequency, time.Duration(that.conf.Producer.FlushMaxMessages)).
		SetRetry(that.conf.Producer.RetryMax).
		SetTimeout(time.Duration(that.conf.Producer.Timeout))

	// 设置topic
	topics := make([]*kafka.Topic, 0, len(that.conf.Producer.Topics))
	for _, topic := range that.conf.Producer.Topics {
		topics = append(topics, kafka.NewTopic(topic.Name).SetOffset(int32(topic.Partition), topic.Offset))
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

	publisher, err := kafka.NewAsyncProducer(subCtx, kafkaConfig, that.logf)
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
	that.cancel()
	that.publisher.Close()
	that.status = idl.ServiceStatusStopped // 设置服务状态为停止状态
	that.publisher = nil
	time.Sleep(500 * time.Millisecond)
	return nil
}

func (that *KafkaMQ) Broadcast(message []byte, properties map[string]string) bool {
	for _, topic := range that.conf.Producer.Topics {
		key := ""
		if properties != nil {
			key = properties["key"]
		}
		if !that.PublishMessage(int32(topic.Partition), topic.Name, key, message) {
			if that.logf != nil {
				that.logf(logger.ErrorLevel, kafkamq_tag,
					"publish topic {} partition {} message {} fault",
					topic.Name, topic.Partition, string(message),
				)
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
	return that.PublishMessage(0, topic, key, message)
}

func (that *KafkaMQ) PublishMessage(partition int32, topic, key string, value []byte) bool {
	if that.status != idl.ServiceStatusRunning { //检查服务状态 是否为运行状态
		return false
	}
	that.publisher.PublisData(partition, topic, key, value)
	return true
}

func (that *KafkaMQ) onError(obj interface{}, err error) {
}

func (that *KafkaMQ) onExit(obj interface{}) {
}
