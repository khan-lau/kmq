package target

import (
	"context"
	"time"

	"github.com/khan-lau/kmq/example/bean/config"
	"github.com/khan-lau/kmq/example/service/idl"
	"github.com/khan-lau/kmq/redismq"

	"github.com/khan-lau/kutils/container/kstrings"
	"github.com/khan-lau/kutils/logger"
)

const (
	redismq_tag = "redismq_target"
)

type RedisMQ struct {
	ctx       context.Context
	cancel    context.CancelFunc
	conf      *config.RedisConfig
	name      string // 服务名称
	status    idl.ServiceStatus
	publisher *redismq.RedisPubSub

	logf logger.AppLogFuncWithTag
}

func NewRedisMQ(ctx context.Context, name string, conf *config.RedisConfig, logf logger.AppLogFuncWithTag) (*RedisMQ, error) {
	subCtx, subCancel := context.WithCancel(ctx)

	redisMQ := &RedisMQ{
		ctx:       subCtx,
		cancel:    subCancel,
		conf:      conf,
		name:      name,
		status:    idl.ServiceStatusStopped,
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
				that.logf(logger.ErrorLevel, redismq_tag, "start service {} error: {}", that.name, err)
			}
			that.onError(that.name, err)
		}
	}()
}

func (that *RedisMQ) Start() error {
	if that.status != idl.ServiceStatusStopped { //检查服务状态 是否为停止状态
		return kstrings.Errorf("service {} is not stopped, status={}", that.name, that.status)
	}

	subCtx := context.WithoutCancel(that.ctx)
	redisConf := redismq.NewRedisConfig().
		SetHost(that.conf.Host).
		SetPort(that.conf.Port).
		SetPassword(that.conf.Password).
		SetDB(int(that.conf.DbNum)).
		SetRetry(int(that.conf.Retry)).
		SetTopics(that.conf.Topics...)
	that.publisher = redismq.NewRedisPubSub(subCtx, redisConf, that.logf)
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
	that.cancel()
	that.publisher.Close()
	that.status = idl.ServiceStatusStopped // 设置服务状态为停止状态
	that.publisher = nil
	time.Sleep(500 * time.Millisecond)
	return nil
}

func (that *RedisMQ) Broadcast(message []byte, properties map[string]string) bool {
	for _, topic := range that.conf.Topics {
		if !that.PublishMessage(topic, string(message)) {
			if that.logf != nil {
				that.logf(logger.ErrorLevel, redismq_tag, "publish topic {} message {} fault", topic, string(message))
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
	that.publisher.PublishMessage(topic, message)
	return true
}

func (that *RedisMQ) onError(obj interface{}, err error) {
}

func (that *RedisMQ) onExit(obj interface{}) {
}
