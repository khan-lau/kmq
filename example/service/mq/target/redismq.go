package target

import (
	"time"

	"github.com/khan-lau/kmq/example/bean/config"
	"github.com/khan-lau/kmq/example/service/idl"
	"github.com/khan-lau/kmq/redismq"

	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/container/kstrings"
	klog "github.com/khan-lau/kutils/klogger"
)

const (
	redismq_tag = "redismq_target"
)

type RedisMQ struct {
	ctx       *kcontext.ContextNode
	conf      *config.RedisConfig
	name      string // 服务名称
	status    idl.ServiceStatus
	chanSize  uint
	publisher *redismq.RedisPubSub
	onReady   redismq.ReadyCallbackFunc
	logf      klog.AppLogFuncWithTag
}

func NewRedisMQ(ctx *kcontext.ContextNode, name string, conf *config.RedisConfig, chanSize uint, logf klog.AppLogFuncWithTag) (*RedisMQ, error) {
	subCtx := ctx.NewChild(kstrings.FormatString("{}_{}", redismq_tag, name))

	redisMQ := &RedisMQ{
		ctx:       subCtx,
		conf:      conf,
		name:      name,
		status:    idl.ServiceStatusStopped,
		chanSize:  chanSize,
		publisher: nil,
		logf:      logf,
	}

	return redisMQ, nil
}

func (that *RedisMQ) Name() string {
	return that.name
}

func (that *RedisMQ) Init() {}

func (that *RedisMQ) StartAsync() {
	go func() {
		err := that.Start()
		if err != nil {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, redismq_tag, "start service {} error: {}", that.name, err)
			}
			that.onError(that.name, err)
		}
	}()
}

func (that *RedisMQ) Start() error {
	if that.status != idl.ServiceStatusStopped { //检查服务状态 是否为停止状态
		return kstrings.Errorf("service {} is not stopped, status={}", that.name, that.status)
	}

	redisConf := redismq.NewRedisConfig().
		SetHost(that.conf.Host).
		SetPort(that.conf.Port).
		SetPassword(that.conf.Password).
		SetDB(int(that.conf.DbNum)).
		SetRetry(int(that.conf.Retry)).
		SetTopics(that.conf.Topics...)
	that.publisher = redismq.NewRedisPubSub(that.ctx, that.chanSize, redisConf, that.logf)

	redisConf.SetReadyCallback(func(ready bool) {
		if that.onReady != nil {
			that.onReady(ready)
		}
	})

	redisConf.SetExitCallback(func(event interface{}) {
		that.onExit(event)
	})

	redisConf.SetErrorCallback(func(err error) {
		that.onError(that.name, err)
	})
	go func() {
		// sleep 500ms, 等待服务启动完成
		time.Sleep(500 * time.Millisecond)
		that.status = idl.ServiceStatusRunning //设置服务状态为运行状态
	}()
	that.publisher.Start()
	return nil
}

func (that *RedisMQ) Restart() error {
	if that.status != idl.ServiceStatusRunning { //检查服务状态 是否为运行状态
		err := that.Stop()
		if err != nil {
			return err
		}
	}

	err := that.Start()
	return err
}

func (that *RedisMQ) Stop() error {
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

func (that *RedisMQ) Broadcast(message []byte, properties map[string]string) bool {
	for _, topic := range that.conf.Topics {
		if !that.PublishMessage(topic, string(message)) {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, redismq_tag, "publish topic {} message {} fault", topic, string(message))
			}
		}
	}
	return true
}

func (that *RedisMQ) Publish(topic string, message []byte, _ map[string]string) bool {
	return that.PublishMessage(topic, string(message))
}

func (that *RedisMQ) PublishMessage(topic string, message string) bool {
	if that.status != idl.ServiceStatusRunning {
		return false
	}
	return that.publisher.PublishMessage(topic, message)
}

func (that *RedisMQ) onError(obj interface{}, err error) {
}

func (that *RedisMQ) onExit(obj interface{}) {
}

func (that *RedisMQ) SetOnReady(callback redismq.ReadyCallbackFunc) *RedisMQ {
	that.onReady = callback
	return that
}
