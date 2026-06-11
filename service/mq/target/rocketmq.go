package target

import (
	"fmt"
	"time"

	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/khan-lau/kmq-utils/rocketmq"
	"github.com/khan-lau/kmq/service/idl"
	"github.com/khan-lau/kmq/service/mq/config"
	"github.com/khan-lau/kutils/container/kcontext"
	kdata "github.com/khan-lau/kutils/data"
	klog "github.com/khan-lau/kutils/klogger"
)

const (
	rocketmq_tag = "rocketmq_target"
)

type RocketMQ struct {
	ctx            *kcontext.ContextNode
	conf           *config.RocketConfig
	name           string // 服务名称
	status         idl.ServiceStatus
	rocketBuffSize uint
	isCompress     bool
	publisher      *rocketmq.Producer
	onReady        rocketmq.ReadyCallbackFunc
	logf           klog.AppLogFuncWithTag
}

func NewRocketMQ(ctx *kcontext.ContextNode, name string, conf *config.RocketConfig, rocketBuffSize uint, isCompress bool, logf klog.AppLogFuncWithTag) (*RocketMQ, error) {
	subCtx := ctx.NewChild(fmt.Sprintf("%s_%s", rocketmq_tag, name))

	rocketMQ := &RocketMQ{
		ctx:            subCtx,
		conf:           conf,
		name:           name,
		status:         idl.ServiceStatusStopped,
		rocketBuffSize: rocketBuffSize,
		isCompress:     isCompress,
		publisher:      nil,
		logf:           logf,
	}

	return rocketMQ, nil
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

	rabbitProducerConfig := rocketmq.NewRocketProducerConfig().
		SetTopics(that.conf.Producer.Topics...).
		SetTimeout((time.Duration(that.conf.Producer.Timeout) * time.Millisecond)).
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
		return fmt.Errorf("unknown queue selector: %s", that.conf.Producer.QueueSelector)
	}

	rocketConfig := rocketmq.NewRocketConfig().
		SetClientID(that.conf.ClientID).
		SetGroupName(that.conf.GroupName).
		SetNamespace(that.conf.Namespace).
		SetCredentialsKey(that.conf.AccessKey, that.conf.SecretKey).
		SetServers(that.conf.Servers...).
		SetNsResolver(that.conf.NsResolver).
		SetProducer(rabbitProducerConfig)

	publisher, err := rocketmq.NewProducer(that.ctx, that.rocketBuffSize, rocketConfig, that.logf)
	if err != nil {
		return err
	}

	rocketConfig.SetReadyCallback(func(ready bool) {
		if that.onReady != nil {
			that.onReady(ready)
		}
	})
	rocketConfig.SetExitCallback(func(event any) {
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

func (that *RocketMQ) Broadcast(message []byte, properties map[string]string) bool {
	var buffer []byte
	if that.isCompress {
		if content, err := kdata.Zip([]byte(message)); err == nil {
			buffer = content
		} else {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, kafkamq_tag, "compress message error: %v", err)
			}
			return false
		}
	} else {
		buffer = message
	}

	for _, topic := range that.conf.Producer.Topics {
		if !that.Publish(topic, buffer, properties) {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, redismq_tag, "publish topic %s message %s fault", topic, string(message))
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
	return that.publisher.PublishData(topic, message, properties)
}

func (that *RocketMQ) onError(obj any, err error) {
}

func (that *RocketMQ) onExit(obj any) {
}

func (that *RocketMQ) SetOnReady(callback rocketmq.ReadyCallbackFunc) *RocketMQ {
	that.onReady = callback
	return that
}
