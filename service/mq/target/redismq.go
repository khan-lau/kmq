package target

import (
	"fmt"
	"time"

	"github.com/khan-lau/kmq-utils/redismq"
	"github.com/khan-lau/kmq/service/idl"
	"github.com/khan-lau/kmq/service/mq/config"
	"github.com/khan-lau/kutils/container/kcontext"
	kdata "github.com/khan-lau/kutils/data"
	klog "github.com/khan-lau/kutils/klogger"
)

const (
	RedisTargetLogTag = "redismq_target"
)

type RedisMQ struct {
	ctx           *kcontext.ContextNode
	conf          *config.RedisConfig
	name          string // 服务名称
	redisBuffSize uint
	isCompress    bool
	status        idl.ServiceStatus
	publisher     *redismq.RedisPub
	onReady       redismq.ReadyCallbackFunc
	logf          klog.AppLogFuncWithTag
}

func NewRedisMQ(ctx *kcontext.ContextNode, name string, conf *config.RedisConfig, redisBuffSize uint, isCompress bool, logf klog.AppLogFuncWithTag) (*RedisMQ, error) {
	subCtx := ctx.NewChild(fmt.Sprintf("%s_%s", RedisTargetLogTag, name))

	redisMQ := &RedisMQ{
		ctx:           subCtx,
		conf:          conf,
		name:          name,
		redisBuffSize: redisBuffSize,
		isCompress:    isCompress,
		status:        idl.ServiceStatusStopped,
		publisher:     nil,
		logf:          logf,
	}
	_ = redisMQ.SetOnReady(func(ready bool) {
		if redisMQ.onReady != nil {
			redisMQ.onReady(ready)
		}
	})
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
				that.logf(klog.ErrorLevel, RedisTargetLogTag, "start service %s error: %v", that.name, err)
			}
			that.onError(that.name, err)
		}
	}()
}

func (that *RedisMQ) Start() error {
	if that.status != idl.ServiceStatusStopped { //检查服务状态 是否为停止状态
		return fmt.Errorf("service %s is not stopped, status=%v", that.name, that.status)
	}

	redisConf := redismq.NewRedisConfig().
		SetAddrs(that.conf.Addrs...).
		SetPassword(that.conf.Password).
		SetDB(int(that.conf.DbNum)).
		SetRetry(int(that.conf.Retry)).
		SetTopics(that.conf.Topics...)

	redisConf.SetReadyCallback(func(ready bool) {
		if that.onReady != nil {
			that.onReady(ready)
		}
	})

	redisConf.SetExitCallback(func(event any) { that.onExit(event) })
	redisConf.SetErrorCallback(func(err error) { that.onError(that.name, err) })

	var err error
	that.publisher, err = redismq.NewRedisPub(that.ctx, that.redisBuffSize, redisConf, that.logf)
	if err != nil {
		return err
	}

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
	var buffer []byte
	if that.isCompress {
		if content, err := kdata.Zip([]byte(message)); err == nil {
			buffer = content
		} else {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, RedisTargetLogTag, "compress message error: %v", err)
			}
			return false
		}
	} else {
		buffer = message
	}
	for _, topic := range that.conf.Topics {
		if !that.PublishMessage(topic, string(buffer)) {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, RedisTargetLogTag, "publish topic %s message %s fault", topic, string(message))
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

func (that *RedisMQ) onError(obj any, err error) {
}

func (that *RedisMQ) onExit(obj any) {
}

func (that *RedisMQ) SetOnReady(callback redismq.ReadyCallbackFunc) *RedisMQ {
	that.onReady = callback
	return that
}
