package source

import (
	"context"
	"khan/kmq/example/bean/config"
	"khan/kmq/example/service/idl"
	"khan/kmq/internal/utils/mq/kafka"
	"time"

	"github.com/khan-lau/kutils/container/kstrings"
	"github.com/khan-lau/kutils/logger"
)

type KafkaMQ struct {
	ctx        context.Context
	cancel     context.CancelFunc
	conf       *config.KafkaConfig
	name       string // 服务名称
	status     idl.ServiceStatus
	subscriber *kafka.Consumer

	logf              logger.AppLogFuncWithTag
	OnRecivedCallback idl.OnRecived // 消息接收回调函数
}

const (
	kafkamq_tag = "kafkamq_source"
)

func NewKafkaMQ(ctx context.Context, name string, conf *config.KafkaConfig, logf logger.AppLogFuncWithTag) (*KafkaMQ, error) {
	subCtx, subCancel := context.WithCancel(ctx)

	rabbitMQ := &KafkaMQ{
		ctx:        subCtx,
		cancel:     subCancel,
		conf:       conf,
		name:       name,
		status:     idl.ServiceStatusStopped,
		subscriber: nil,
		logf:       logf,
	}

	return rabbitMQ, nil
}

func (that *KafkaMQ) Name() string {
	return that.name
}

func (that *KafkaMQ) Init() {}

func (that *KafkaMQ) SetOnRecivedCallback(callback idl.OnRecived) {
	that.OnRecivedCallback = callback
}

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

	kafkaConsumerConfig := kafka.NewKafkaConsumerConfig().
		SetSessionTimeout(time.Duration(that.conf.Consumer.SessionTimeout)).
		SetHeartbeatInterval(time.Duration(that.conf.Consumer.HeartbeatInterval)).
		SetInitialOffset(that.conf.Consumer.InitialOffset).
		SetRebalanceTimeout(time.Duration(that.conf.Consumer.RebalanceTimeout)).
		SetFetch(that.conf.Consumer.Min, that.conf.Consumer.Max, that.conf.Consumer.Fetch).
		SetAssignor(that.conf.Consumer.Assignor)

	// TODO 载入上次消费的offset
	topics := make([]*kafka.Topic, 0, len(that.conf.Consumer.Topics))
	for _, topic := range that.conf.Consumer.Topics {
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
		SetConsumer(kafkaConsumerConfig)

	subscriber, err := kafka.NewConsumer(subCtx, kafkaConfig, that.logf)
	if err != nil {
		return err
	}

	kafkaConfig.SetExitCallback(func(event interface{}) {
		that.onExit(event)
	})

	kafkaConfig.SetErrorCallback(func(err error) {
		that.onError(that.name, err)
	})
	that.subscriber = subscriber
	go func() {
		// sleep 500ms, 等待服务启动完成
		time.Sleep(500 * time.Millisecond)
		that.status = idl.ServiceStatusRunning //设置服务状态为运行状态
	}()
	that.subscriber.SyncSubscribe(nil, func(voidObj interface{}, msg *kafka.KafkaMessage) {
		keyMap := map[string]string{"key": string(msg.Key)}
		that.OnRecved(msg.Topic, int(msg.Partition), msg.Offset, keyMap, msg.Value)
	})

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
	that.subscriber.Close()
	that.status = idl.ServiceStatusStopped // 设置服务状态为停止状态
	that.subscriber = nil
	time.Sleep(500 * time.Millisecond)
	return nil
}

func (that *KafkaMQ) onError(obj interface{}, err error) {
}

func (that *KafkaMQ) onExit(obj interface{}) {
}

func (that *KafkaMQ) OnRecved(topic string, partition int, offset int64, properties map[string]string, message []byte) {
	if that.OnRecivedCallback != nil {
		that.OnRecivedCallback(that.Name(), topic, partition, offset, properties, message)
	}
}
