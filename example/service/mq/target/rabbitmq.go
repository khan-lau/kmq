package target

import (
	"context"
	"time"

	"github.com/khan-lau/kmq/example/bean/config"
	"github.com/khan-lau/kmq/example/service/idl"
	"github.com/khan-lau/kmq/rabbitmq"

	"github.com/khan-lau/kutils/container/kstrings"
	klog "github.com/khan-lau/kutils/klogger"
)

type RabbitMQ struct {
	ctx       context.Context
	cancel    context.CancelFunc
	conf      *config.RabbitConfig
	name      string // 服务名称
	status    idl.ServiceStatus
	publisher *rabbitmq.Producer

	logf klog.AppLogFuncWithTag
}

const (
	rabbitmq_tag = "rabbitmq_target"
)

func NewRabbitMQ(ctx context.Context, name string, conf *config.RabbitConfig, logf klog.AppLogFuncWithTag) (*RabbitMQ, error) {
	subCtx, subCancel := context.WithCancel(ctx)

	rabbitMQ := &RabbitMQ{
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

func (that *RabbitMQ) Name() string {
	return that.name
}

func (that *RabbitMQ) Init() {}

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

	// rabbitConsumerConfig := rabbitmq.NewConsumerConfig().
	// 	SetExchange(that.conf.Consumer.Exchange).
	// 	SetQueueName(that.conf.Consumer.QueueName).
	// 	SetRouterKey(that.conf.Consumer.KRouterKey).
	// 	SetWorkType(that.conf.Consumer.WorkType)
	rabbitProducerConfig := rabbitmq.NewProducerConfig().
		SetExchange(that.conf.Producer.Exchange).
		SetRouter(that.conf.Producer.Router).
		SetWorkType(that.conf.Producer.WorkType)

	rabbitConfig := rabbitmq.NewRabbitConfig().
		SetUser(that.conf.User).
		SetPassword(that.conf.Password).
		SetHost(that.conf.Host).
		SetPort(that.conf.Port).
		SetVHost(that.conf.VHost).
		SetProducer(rabbitProducerConfig)

	publisher, err := rabbitmq.NewProducer(that.ctx, rabbitConfig, that.logf)
	if err != nil {
		return err
	}

	rabbitConfig.SetExitCallback(func(event interface{}) {
		that.onExit(event)
	})

	rabbitConfig.SetErrorCallback(func(err error) {
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
	that.cancel()
	that.publisher.Close()
	that.status = idl.ServiceStatusStopped // 设置服务状态为停止状态
	that.publisher = nil
	time.Sleep(500 * time.Millisecond)
	return nil
}

func (that *RabbitMQ) Broadcast(message []byte, _ map[string]string) bool {
	return that.Publish(that.conf.Producer.Router, message, nil)
}

func (that *RabbitMQ) Publish(router string, message []byte, _ map[string]string) bool {
	return that.PublishMessage(that.conf.Producer.Exchange, router, string(message))
}

func (that *RabbitMQ) PublishMessage(exchange string, router string, message string) bool {
	if that.status != idl.ServiceStatusRunning { //检查服务状态 是否为运行状态
		return false
	}
	that.publisher.PublishMessage(exchange, router, message)
	return true
}

func (that *RabbitMQ) onError(obj interface{}, err error) {
}

func (that *RabbitMQ) onExit(obj interface{}) {
}
