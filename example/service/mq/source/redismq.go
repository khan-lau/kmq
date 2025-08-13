package source

import (
	"time"

	"github.com/khan-lau/kmq/example/bean/config"
	"github.com/khan-lau/kmq/example/service/idl"
	"github.com/khan-lau/kmq/redismq"

	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/container/kstrings"
	"github.com/khan-lau/kutils/db/kredis"
	klog "github.com/khan-lau/kutils/klogger"
)

type RedisMQ struct {
	ctx        *kcontext.ContextNode
	conf       *config.RedisConfig
	name       string // 服务名称
	status     idl.ServiceStatus
	subscriber *redismq.RedisPubSub

	logf              klog.AppLogFuncWithTag
	OnRecivedCallback idl.OnRecived // 消息接收回调函数
}

const (
	redismq_tag = "redismq_source"
)

func NewRedisMQ(ctx *kcontext.ContextNode, name string, conf *config.RedisConfig, logf klog.AppLogFuncWithTag) (*RedisMQ, error) {
	subCtx := ctx.NewChild(kstrings.FormatString("{}_{}", redismq_tag, name))

	redisMQ := &RedisMQ{
		ctx:        subCtx,
		conf:       conf,
		name:       name,
		status:     idl.ServiceStatusStopped,
		subscriber: nil,
		logf:       logf,
	}

	return redisMQ, nil
}

func (that *RedisMQ) Name() string {
	return that.name
}

func (that *RedisMQ) Init() {}

func (that *RedisMQ) SetOnRecivedCallback(callback idl.OnRecived) {
	that.OnRecivedCallback = callback
}

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
	that.subscriber = redismq.NewRedisPubSub(that.ctx, 20000, redisConf, that.logf)
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
	that.subscriber.SyncSubscribe(nil, func(voidObj interface{}, msg *kredis.RedisMessage) {
		that.OnRecved(nil, msg.Topic, 0, 0, nil, []byte(msg.Message))
	})

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
	if that.subscriber != nil {
		that.subscriber.Close()
	}
	that.status = idl.ServiceStatusStopped // 设置服务状态为停止状态
	that.subscriber = nil
	time.Sleep(500 * time.Millisecond)
	that.ctx.Remove()
	return nil
}

func (that *RedisMQ) onError(obj interface{}, err error) {
}

func (that *RedisMQ) onExit(obj interface{}) {
}

func (that *RedisMQ) OnRecved(origin interface{}, topic string, partition int, offset int64, properties map[string]string, message []byte) {
	if that.OnRecivedCallback != nil {
		that.OnRecivedCallback(origin, that.Name(), topic, partition, offset, properties, message)
	}
}
