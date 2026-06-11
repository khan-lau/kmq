package source

import (
	"fmt"
	"time"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/khan-lau/kmq-utils/rocketmq"
	"github.com/khan-lau/kmq/service/idl"
	"github.com/khan-lau/kmq/service/mq/config"
	"github.com/khan-lau/kutils/container/kcontext"
	klog "github.com/khan-lau/kutils/klogger"
)

const (
	rocketmq_tag = "rocketmq_source"
)

type RocketMQ struct {
	ctx            *kcontext.ContextNode
	conf           *config.RocketConfig
	name           string // 服务名称
	rocketBuffSize uint
	status         idl.ServiceStatus
	subscriber     *rocketmq.PushConsumer

	logf              klog.AppLogFuncWithTag
	OnRecivedCallback idl.OnRecived // 消息接收回调函数
}

func NewRocketMQ(ctx *kcontext.ContextNode, name string, conf *config.RocketConfig, rocketBuffSize uint, logf klog.AppLogFuncWithTag) (*RocketMQ, error) {
	subCtx := ctx.NewChild(fmt.Sprintf("%s_%s", rocketmq_tag, name))

	rocketMQ := &RocketMQ{
		ctx:            subCtx,
		conf:           conf,
		name:           name,
		rocketBuffSize: rocketBuffSize,
		status:         idl.ServiceStatusStopped,
		subscriber:     nil,
		logf:           logf,
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
				that.logf(klog.ErrorLevel, rocketmq_tag, "start service %s error: %v", that.name, err)
			}
			that.onError(that.name, err)
		}
	}()
}

func (that *RocketMQ) Start() error {
	if that.status != idl.ServiceStatusStopped { //检查服务状态 是否为停止状态
		return fmt.Errorf("service %s is not stopped, status=%v", that.name, that.status)
	}

	rocketConsumerConfig := rocketmq.NewRocketConsumerConfig().
		SetTopics(that.conf.Consumer.Topics...).
		SetOrder(that.conf.Consumer.Order).
		SetMessageBatchMaxSize(that.conf.Consumer.MessageBatchMaxSize).
		SetMaxReconsumeTimes(that.conf.Consumer.MaxReconsumeTimes).
		SetAutoCommit(that.conf.Consumer.AutoCommit)

	switch that.conf.Consumer.Mode {
	case "Clustering":
		rocketConsumerConfig.SetMode(consumer.Clustering)
	case "BroadCasting":
		rocketConsumerConfig.SetMode(consumer.BroadCasting)
	default:
		return fmt.Errorf("unknown consumer mode: %s", that.conf.Consumer.Mode)
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
			return fmt.Errorf("consumer offset is ConsumeFromTimestamp, but timestamp is empty")
		}

	default:
		return fmt.Errorf("unknown consumer offset: %s", that.conf.Consumer.Offset)
	}

	rocketConfig := rocketmq.NewRocketConfig().
		SetClientID(that.conf.ClientID).
		SetGroupName(that.conf.GroupName).
		SetNamespace(that.conf.Namespace).
		SetCredentialsKey(that.conf.AccessKey, that.conf.SecretKey).
		SetServers(that.conf.Servers...).
		SetNsResolver(that.conf.NsResolver).
		SetConsumer(rocketConsumerConfig)

	subscriber, err := rocketmq.NewPushConsumer(that.ctx, that.rocketBuffSize, rocketConfig, that.logf)
	if err != nil {
		return err
	}

	rocketConfig.SetExitCallback(func(event any) {
		that.onExit(event)
	})

	rocketConfig.SetErrorCallback(func(err error) {
		that.onError(that.name, err)
	})

	rocketConfig.Consumer.SetMainHandler(func(voidObj any, msg *rocketmq.Message) {
		that.OnRecved(msg, msg.Topic, 0, int64(msg.StoreTimestamp), msg.GetProperties(), msg.Body)
	})

	that.subscriber = subscriber
	go func() {
		// sleep 500ms, 等待服务启动完成
		time.Sleep(500 * time.Millisecond)
		that.status = idl.ServiceStatusRunning //设置服务状态为运行状态
	}()

	that.subscriber.SyncSubscribe()

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
	that.ctx.Cancel()
	if that.subscriber != nil {
		that.subscriber.Close()
	}
	that.status = idl.ServiceStatusStopped // 设置服务状态为停止状态
	that.subscriber = nil
	time.Sleep(500 * time.Millisecond)
	that.ctx.Remove()
	return nil
}

func (that *RocketMQ) onError(obj any, err error) {
}

func (that *RocketMQ) onExit(obj any) {
}

func (that *RocketMQ) OnRecved(origin any, topic string, partition int, offset int64, properties map[string]string, message []byte) {
	if that.OnRecivedCallback != nil {
		that.OnRecivedCallback(origin, that.Name(), topic, partition, offset, properties, message)
	}
}
