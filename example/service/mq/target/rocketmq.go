package target

import (
	"context"
	"time"

	"github.com/khan-lau/kmq/example/bean/config"
	"github.com/khan-lau/kmq/example/service/idl"
	"github.com/khan-lau/kmq/rocketmq"

	"github.com/apache/rocketmq-client-go/v2/producer"

	"github.com/khan-lau/kutils/container/kstrings"
	"github.com/khan-lau/kutils/logger"
)

type RocketMQ struct {
	ctx       context.Context
	cancel    context.CancelFunc
	conf      *config.RocketConfig
	name      string // 服务名称
	status    idl.ServiceStatus
	publisher *rocketmq.Producer

	logf logger.AppLogFuncWithTag
}

const (
	rocketmq_tag = "rocketmq_target"
)

func NewRocketMQ(ctx context.Context, name string, conf *config.RocketConfig, logf logger.AppLogFuncWithTag) (*RocketMQ, error) {
	subCtx, subCancel := context.WithCancel(ctx)

	rabbitMQ := &RocketMQ{
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

func (that *RocketMQ) Name() string {
	return that.name
}

func (that *RocketMQ) Init() {}

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

	rabbitProducerConfig := rocketmq.NewRocketProducerConfig().
		SetTopics(that.conf.Producer.Topics...).
		SetTimeout(that.conf.Producer.Timeout).
		SetRetry(that.conf.Producer.Retry).
		SetAsyncSend(that.conf.Producer.AsyncSend)
	switch that.conf.Producer.QueueSelector {
	case "RandomQueueSelector":
		rabbitProducerConfig.SetQueueSelector(producer.NewRandomQueueSelector())
	case "RoundRobinQueueSelector":
		rabbitProducerConfig.SetQueueSelector(producer.NewRoundRobinQueueSelector())
	case "ManualQueueSelector":
		rabbitProducerConfig.SetQueueSelector(producer.NewManualQueueSelector())
	default: // NewManualQueueSelector
		return kstrings.Errorf("unknown queue selector: {}", that.conf.Producer.QueueSelector)
	}

	rocketConfig := rocketmq.NewRocketConfig().
		SetClientID(that.conf.ClientID).
		SetGroupName(that.conf.GroupName).
		SetNamespace(that.conf.Namespace).
		SetCredentialsKey(that.conf.AccessKey, that.conf.SecretKey).
		SetServers(that.conf.Servers...).
		SetNsResolver(that.conf.NsResolver).
		SetProducer(rabbitProducerConfig)

	publisher, err := rocketmq.NewProducer(subCtx, rocketConfig, that.logf)
	if err != nil {
		return err
	}

	rocketConfig.SetExitCallback(func(event interface{}) {
		that.onExit(event)
	})

	rocketConfig.SetErrorCallback(func(err error) {
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
	that.publisher.Close()
	that.status = idl.ServiceStatusStopped // 设置服务状态为停止状态
	that.publisher = nil
	time.Sleep(500 * time.Millisecond)
	return nil
}

func (that *RocketMQ) Broadcast(message []byte, properties map[string]string) bool {
	for _, topic := range that.conf.Producer.Topics {
		if !that.Publish(topic, message, properties) {
			if that.logf != nil {
				that.logf(logger.ErrorLevel, redismq_tag, "publish topic {} message {} fault", topic, string(message))
			}
		}
	}
	return true
}

func (that *RocketMQ) Publish(topic string, message []byte, properties map[string]string) bool {
	return that.PublishMessage(topic, message, properties)
}

func (that *RocketMQ) PublishMessage(topic string, message []byte, properties map[string]string) bool {
	if that.status != idl.ServiceStatusRunning {
		return false
	}
	that.publisher.PublishData(topic, message, properties)
	return true
}

func (that *RocketMQ) onError(obj interface{}, err error) {
}

func (that *RocketMQ) onExit(obj interface{}) {
}
