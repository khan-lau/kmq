package target

import (
	"fmt"
	"time"

	"github.com/khan-lau/kmq-utils/rabbitmq"
	"github.com/khan-lau/kmq/service/idl"
	"github.com/khan-lau/kmq/service/mq/config"
	"github.com/khan-lau/kutils/container/kcontext"
	kdata "github.com/khan-lau/kutils/data"
	klog "github.com/khan-lau/kutils/klogger"
)

const (
	RabbitTargetLogTag = "rabbitmq_target"
)

type RabbitMQ struct {
	ctx            *kcontext.ContextNode
	conf           *config.RabbitConfig
	name           string // 服务名称
	status         idl.ServiceStatus
	rabbitBuffSize uint
	isCompress     bool
	publisher      *rabbitmq.Producer
	onReady        rabbitmq.ReadyCallbackFunc
	logf           klog.AppLogFuncWithTag
}

func NewRabbitMQ(ctx *kcontext.ContextNode, name string, conf *config.RabbitConfig, rabbitBuffSize uint, isCompress bool, logf klog.AppLogFuncWithTag) (*RabbitMQ, error) {
	subCtx := ctx.NewChild(fmt.Sprintf("%s_%s", RabbitTargetLogTag, name))

	rabbitMQ := &RabbitMQ{
		ctx:            subCtx,
		conf:           conf,
		name:           name,
		rabbitBuffSize: rabbitBuffSize,
		status:         idl.ServiceStatusStopped,
		isCompress:     isCompress,
		publisher:      nil,
		logf:           logf,
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
			that.log(klog.ErrorLevel, "start service %s error: %v", that.name, err)
			that.onError(that.name, err)
		}
	}()
}

func (that *RabbitMQ) Start() error {
	if that.status != idl.ServiceStatusStopped { //检查服务状态 是否为停止状态
		return fmt.Errorf("service %s is not stopped, status=%v", that.name, that.status)
	}

	rabbitProducerConfig := rabbitmq.NewProducerConfig().
		SetExchange(that.conf.Producer.Exchange).
		SetRouter(that.conf.Producer.Router).
		SetWorkType(that.conf.Producer.WorkType).
		SetReturnAck(that.conf.Producer.ReturnAck)

	rabbitConfig := rabbitmq.NewRabbitConfig().
		SetUser(that.conf.User).
		SetPassword(that.conf.Password).
		SetAddrs(that.conf.Addrs...).
		SetVHost(that.conf.VHost).
		SetProducer(rabbitProducerConfig)

	rabbitConfig.SetReadyCallback(func(ready bool) {
		if that.onReady != nil {
			that.onReady(ready)
		}
	})

	rabbitConfig.SetExitCallback(func(event any) { that.onExit(event) })
	rabbitConfig.SetErrorCallback(func(err error) { that.onError(that.name, err) })

	publisher, err := rabbitmq.NewProducer(that.ctx, that.rabbitBuffSize, rabbitConfig, that.logf)
	if err != nil {
		return err
	}

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

func (that *RabbitMQ) Broadcast(message []byte, _ map[string]string) bool {
	var buffer []byte
	if that.isCompress {
		if content, err := kdata.Zip([]byte(message)); err == nil {
			buffer = content
		} else {
			that.log(klog.ErrorLevel, "compress message error: %v", err)
			return false
		}
	} else {
		buffer = message
	}
	return that.Publish(that.conf.Producer.Router, buffer, nil)
}

func (that *RabbitMQ) Publish(router string, message []byte, _ map[string]string) bool {
	return that.PublishMessage(that.conf.Producer.Exchange, router, string(message))
}

func (that *RabbitMQ) PublishMessage(exchange string, router string, message string) bool {
	if that.status != idl.ServiceStatusRunning { //检查服务状态 是否为运行状态
		return false
	}
	return that.publisher.PublishMessage(exchange, router, message)
}

func (that *RabbitMQ) onError(obj any, err error) {
}

func (that *RabbitMQ) onExit(obj any) {
}

func (that *RabbitMQ) SetOnReady(callback rabbitmq.ReadyCallbackFunc) *RabbitMQ {
	that.onReady = callback
	return that
}

// log 日志记录, 会自动添加 RabbitTargetLogTag
//
//go:inline
func (that *RabbitMQ) log(level klog.Level, format string, args ...any) {
	if that.logf != nil {
		that.logf(level, RabbitTargetLogTag, 1, format, args...)
	}
}
