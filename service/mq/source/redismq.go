package source

import (
	"fmt"
	"time"

	"github.com/khan-lau/kmq-utils/redismq"
	"github.com/khan-lau/kmq/service/idl"
	"github.com/khan-lau/kmq/service/mq/config"
	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/db/kredis"
	klog "github.com/khan-lau/kutils/klogger"
)

const (
	RedisSourceLogTag = "redismq_source"
)

type RedisMQ struct {
	ctx           *kcontext.ContextNode
	conf          *config.RedisConfig
	name          string // 服务名称
	redisBuffSize uint
	status        idl.ServiceStatus
	subscriber    *redismq.RedisSub

	logf              klog.AppLogFuncWithTag
	OnRecivedCallback idl.OnRecived // 消息接收回调函数
}

func NewRedisMQ(ctx *kcontext.ContextNode, name string, conf *config.RedisConfig, redisBuffSize uint, logf klog.AppLogFuncWithTag) (*RedisMQ, error) {
	subCtx := ctx.NewChild(fmt.Sprintf("%s_%s", RedisSourceLogTag, name))

	redisMQ := &RedisMQ{
		ctx:           subCtx,
		conf:          conf,
		name:          name,
		redisBuffSize: redisBuffSize,
		status:        idl.ServiceStatusStopped,
		subscriber:    nil,
		logf:          logf,
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
			that.log(klog.ErrorLevel, "start service %s error: %v", that.name, err)
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

	redisConf.SetExitCallback(func(event any) { that.onExit(event) })
	redisConf.SetErrorCallback(func(err error) { that.onError(that.name, err) })
	redisConf.SetMessageHandler(func(voidObj any, msg *kredis.RedisMessage) {
		that.OnRecved(nil, msg.Topic, 0, 0, nil, []byte(msg.Message))
	})

	var err error
	that.subscriber, err = redismq.NewRedisSub(that.ctx, that.redisBuffSize, redisConf, that.logf)
	if err != nil {
		return err
	}

	go func() {
		// sleep 500ms, 等待服务启动完成
		time.Sleep(500 * time.Millisecond)
		that.status = idl.ServiceStatusRunning //设置服务状态为运行状态
	}()
	that.subscriber.SyncSubscribe()

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

func (that *RedisMQ) onError(obj any, err error) {
}

func (that *RedisMQ) onExit(obj any) {
}

func (that *RedisMQ) OnRecved(origin any, topic string, partition int, offset int64, properties map[string]string, message []byte) {
	if that.OnRecivedCallback != nil {
		that.OnRecivedCallback(origin, that.Name(), topic, partition, offset, properties, message)
	}
}

func (that *RedisMQ) SetOnRecivedCallback(callback idl.OnRecived) {
	that.OnRecivedCallback = callback
}

// log 日志记录, 会自动添加 RedisSourceLogTag
//
//go:inline
func (that *RedisMQ) log(level klog.Level, format string, args ...any) {
	if that.logf != nil {
		that.logf(level, RedisSourceLogTag, format, args...)
	}
}
