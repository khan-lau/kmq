package source

import (
	"context"
	"time"

	"github.com/khan-lau/kmq/example/bean/config"
	"github.com/khan-lau/kmq/example/service/idl"
	"github.com/khan-lau/kmq/internal/utils/mq/rocketmq"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/khan-lau/kutils/container/kstrings"
	"github.com/khan-lau/kutils/logger"
)

type RocketMQ struct {
	ctx        context.Context
	cancel     context.CancelFunc
	conf       *config.RocketConfig
	name       string // 服务名称
	status     idl.ServiceStatus
	subscriber *rocketmq.Consumer

	logf              logger.AppLogFuncWithTag
	OnRecivedCallback idl.OnRecived // 消息接收回调函数
}

const (
	rocketmq_tag = "rocketmq_source"
)

func NewRocketMQ(ctx context.Context, name string, conf *config.RocketConfig, logf logger.AppLogFuncWithTag) (*RocketMQ, error) {
	subCtx, subCancel := context.WithCancel(ctx)

	rocketMQ := &RocketMQ{
		ctx:        subCtx,
		cancel:     subCancel,
		conf:       conf,
		name:       name,
		status:     idl.ServiceStatusStopped,
		subscriber: nil,
		logf:       logf,
	}

	return rocketMQ, nil
}

func (that *RocketMQ) Name() string {
	return that.name
}

func (that *RocketMQ) Init() {}

func (that *RocketMQ) SetOnRecivedCallback(callback idl.OnRecived) {
	that.OnRecivedCallback = callback
}

func (that *RocketMQ) StartAsync() {
	go func() {
		err := that.Start()
		if err != nil {
			if that.logf != nil {
				that.logf(logger.ErrorLevel, rocketmq_tag, "start service {} error: {}", that.name, err)
			}
			that.onError(that.name, err)
		}
	}()
}

func (that *RocketMQ) Start() error {
	if that.status != idl.ServiceStatusStopped { //检查服务状态 是否为停止状态
		return kstrings.Errorf("service {} is not stopped, status={}", that.name, that.status)
	}

	subCtx := context.WithoutCancel(that.ctx)

	rocketConsumerConfig := rocketmq.NewRocketConsumerConfig().
		SetTopics(that.conf.Consumer.Topics...).
		SetOrder(that.conf.Consumer.Order).
		SetMessageBatchMaxSize(that.conf.Consumer.MessageBatchMaxSize).
		SetMaxReconsumeTimes(that.conf.Consumer.MaxReconsumeTimes)

	switch that.conf.Consumer.Mode {
	case "Clustering":
		rocketConsumerConfig.SetMode(consumer.Clustering)
	case "BroadCasting":
		rocketConsumerConfig.SetMode(consumer.BroadCasting)
	default:
		return kstrings.Errorf("unknown consumer mode: {}", that.conf.Consumer.Mode)
	}

	switch that.conf.Consumer.Offset {
	case "ConsumeFromFirstOffset":
		rocketConsumerConfig.SetOffset(consumer.ConsumeFromFirstOffset)
	case "ConsumeFromLastOffset":
		rocketConsumerConfig.SetOffset(consumer.ConsumeFromLastOffset)
	case "ConsumeFromTimestamp":
		rocketConsumerConfig.SetOffset(consumer.ConsumeFromTimestamp)
		if that.conf.Consumer.Timestamp != "" {
			rocketConsumerConfig.SetTimestamp(that.conf.Consumer.Timestamp)
		} else {
			return kstrings.Errorf("consumer offset is ConsumeFromTimestamp, but timestamp is empty")
		}

	default:
		return kstrings.Errorf("unknown consumer offset: {}", that.conf.Consumer.Offset)
	}

	rocketConfig := rocketmq.NewRocketConfig().
		SetClientID(that.conf.ClientID).
		SetGroupName(that.conf.GroupName).
		SetNamespace(that.conf.Namespace).
		SetCredentialsKey(that.conf.AccessKey, that.conf.SecretKey).
		SetServers(that.conf.Servers...).
		SetNsResolver(that.conf.NsResolver).
		SetConsumer(rocketConsumerConfig)

	subscriber, err := rocketmq.NewConsumer(subCtx, rocketConfig, that.logf)
	if err != nil {
		return err
	}

	rocketConfig.SetExitCallback(func(event interface{}) {
		that.onExit(event)
	})

	rocketConfig.SetErrorCallback(func(err error) {
		that.onError(that.name, err)
	})
	that.subscriber = subscriber
	go func() {
		// sleep 500ms, 等待服务启动完成
		time.Sleep(500 * time.Millisecond)
		that.status = idl.ServiceStatusRunning //设置服务状态为运行状态
	}()

	that.subscriber.SyncSubscribe(nil, func(voidObj interface{}, msg *primitive.MessageExt) {
		that.OnRecved(msg.Topic, 0, int64(msg.QueueOffset), msg.GetProperties(), msg.Body)
	})

	return nil
}

func (that *RocketMQ) Restart() error {
	if that.status != idl.ServiceStatusRunning { //检查服务状态 是否为运行状态
		err := that.Stop()
		if err != nil {
			return err
		}
	}

	err := that.Start()
	return err
}

func (that *RocketMQ) Stop() error {
	that.cancel()
	that.subscriber.Close()
	that.status = idl.ServiceStatusStopped // 设置服务状态为停止状态
	that.subscriber = nil
	time.Sleep(500 * time.Millisecond)
	return nil
}

func (that *RocketMQ) onError(obj interface{}, err error) {
}

func (that *RocketMQ) onExit(obj interface{}) {
}

func (that *RocketMQ) OnRecved(topic string, partition int, offset int64, properties map[string]string, message []byte) {
	if that.OnRecivedCallback != nil {
		that.OnRecivedCallback(that.Name(), topic, partition, offset, properties, message)
	}
}
