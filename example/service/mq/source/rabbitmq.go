package source

import (
	"time"

	conf "github.com/khan-lau/kmq/example/bean/config"
	"github.com/khan-lau/kmq/example/service/idl"
	"github.com/khan-lau/kmq/rabbitmq"

	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/container/kstrings"
	klog "github.com/khan-lau/kutils/klogger"
)

type RabbitMQ struct {
	ctx        *kcontext.ContextNode
	conf       *conf.RabbitConfig
	name       string // 服务名称
	status     idl.ServiceStatus
	subscriber *rabbitmq.Consumer

	logf              klog.AppLogFuncWithTag
	OnRecivedCallback idl.OnRecived // 消息接收回调函数
}

const (
	rabbitmq_tag = "rabbitmq_source"
)

func NewRabbitMQ(ctx *kcontext.ContextNode, name string, conf *conf.RabbitConfig, logf klog.AppLogFuncWithTag) (*RabbitMQ, error) {
	subCtx := ctx.NewChild(kstrings.FormatString("{}_{}", rabbitmq_tag, name))

	rabbitMQ := &RabbitMQ{
		ctx:        subCtx,
		conf:       conf,
		name:       name,
		status:     idl.ServiceStatusStopped,
		subscriber: nil,
		logf:       logf,
	}

	return rabbitMQ, nil
}

func (that *RabbitMQ) Name() string {
	return that.name
}

func (that *RabbitMQ) Init() {}

func (that *RabbitMQ) SetOnRecivedCallback(callback idl.OnRecived) {
	that.OnRecivedCallback = callback
}

func (that *RabbitMQ) StartAsync() {
	go func() {
		err := that.Start()
		if err != nil {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, rabbitmq_tag, "start service {} error: {}", that.name, err)
			}
			that.onError(that.name, err)
		}
	}()
}

func (that *RabbitMQ) Start() error {
	if that.status != idl.ServiceStatusStopped { //检查服务状态 是否为停止状态
		return kstrings.Errorf("service {} is not stopped, status={}", that.name, that.status)
	}

	rabbitConsumerConfig := rabbitmq.NewConsumerConfig().
		SetExchange(that.conf.Consumer.Exchange).
		SetQueueName(that.conf.Consumer.QueueName).
		SetRouterKey(that.conf.Consumer.KRouterKey).
		SetWorkType(that.conf.Consumer.WorkType).
		SetAutoCommit(that.conf.Consumer.AutoCommit)

	rabbitConfig := rabbitmq.NewRabbitConfig().
		SetUser(that.conf.User).
		SetPassword(that.conf.Password).
		SetHost(that.conf.Host).
		SetPort(that.conf.Port).
		SetVHost(that.conf.VHost).SetConsumer(rabbitConsumerConfig)

	subscriber, err := rabbitmq.NewConsumer(rabbitConfig, that.logf)
	if err != nil {
		return err
	}

	rabbitConfig.SetExitCallback(func(event interface{}) {
		that.onExit(event)
	})

	rabbitConfig.SetErrorCallback(func(err error) {
		that.onError(that.name, err)
	})
	that.subscriber = subscriber
	go func() {
		// sleep 500ms, 等待服务启动完成
		time.Sleep(500 * time.Millisecond)
		that.status = idl.ServiceStatusRunning //设置服务状态为运行状态
	}()

	that.subscriber.SyncSubscribe(nil, func(voidObj interface{}, msg *rabbitmq.Message) {
		that.OnRecved(msg, msg.RoutingKey, 0, msg.Timestamp.UnixMilli(), nil, []byte(msg.Body))
	})

	return nil
}

func (that *RabbitMQ) Restart() error {
	if that.status != idl.ServiceStatusRunning { //检查服务状态 是否为运行状态
		err := that.Stop()
		if err != nil {
			return err
		}
	}

	err := that.Start()
	return err
}

func (that *RabbitMQ) Stop() error {
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

func (that *RabbitMQ) onError(obj interface{}, err error) {
}

func (that *RabbitMQ) onExit(obj interface{}) {
}

func (that *RabbitMQ) OnRecved(origin interface{}, topic string, partition int, offset int64, properties map[string]string, message []byte) {
	if that.OnRecivedCallback != nil {
		that.OnRecivedCallback(origin, that.Name(), topic, partition, offset, properties, message)
	}
}
