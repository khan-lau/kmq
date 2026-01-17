package redismq

import (
	"time"

	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/container/kstrings"
	"github.com/khan-lau/kutils/db/kredis"
	klog "github.com/khan-lau/kutils/klogger"
)

type SubscribeCallback func(voidObj interface{}, msg *kredis.RedisMessage)

type RedisPubSub struct {
	ctx          *kcontext.ContextNode
	redisHandler *kredis.KRedis
	connected    bool
	queue        chan *kredis.RedisMessage // 消息队列
	chanSize     uint                      // 队列大小
	conf         *RedisConfig
	logf         klog.AppLogFuncWithTag
}

func NewRedisPubSub(ctx *kcontext.ContextNode, chanSize uint, conf *RedisConfig, logf klog.AppLogFuncWithTag) *RedisPubSub {
	subCtx := ctx.NewChild("redismq_pubsub")
	redisHD := kredis.NewKRedis(subCtx.Context(), conf.Host, int(conf.Port), "", conf.Password, conf.DB)
	redisPs := &RedisPubSub{
		ctx:          ctx,
		redisHandler: redisHD,
		connected:    false,
		chanSize:     chanSize,
		queue:        make(chan *kredis.RedisMessage, chanSize),
		conf:         conf,
		logf:         logf,
	}

	return redisPs
}

func (that *RedisPubSub) Subscribe(voidObj interface{}, callback SubscribeCallback) {
	go that.SyncSubscribe(voidObj, callback)
}

func (that *RedisPubSub) SyncSubscribe(voidObj interface{}, callback SubscribeCallback) {
	if that.connected { // 已连接, 不再重连
		if that.logf != nil {
			that.logf(klog.InfoLevel, redis_tag, "Client is connected, do nothing")
		}
		return
	}

	subCtx := that.ctx.NewChild("redismq_sub")
	err := that.connectUtil(subCtx)
	if err != nil {
		if that.logf != nil {
			that.logf(klog.WarnLevel, redis_tag, "Connect error: {}", err)
		}
		if that.conf.OnError != nil {
			that.conf.OnError(err)
		}
		subCtx.Cancel()
		subCtx.Remove()
		return
	}

	if that.conf != nil && that.conf.OnReady != nil {
		that.conf.OnReady(true)
	}

	// consumerErrChan := make(chan error)
	that.redisHandler.PSubscribeWithChanSize(1000, int(that.chanSize),
		func(err error, topic string, payload interface{}) {
			if err != nil {
				if that.logf != nil {
					// TODO 某些指定错误需要重连
					that.logf(klog.WarnLevel, redis_tag, "Subscribe error: {}", err)
				}
				if that.conf.OnError != nil {
					that.conf.OnError(err)
				}
				// consumerErrChan <- err
			} else {
				msg, err := that.receivedMessage(topic, payload)
				if nil != err && that.logf != nil {
					that.logf(klog.WarnLevel, redis_tag, "Subscribe reids topic error: {}", err)
				}
				if callback != nil {
					callback(voidObj, msg)
				}
			}

		}, that.conf.Topics...,
	)

	<-that.ctx.Context().Done()

	subCtx.Cancel()
	subCtx.Remove()
	that.stop()
	if that.logf != nil {
		that.logf(klog.InfoLevel, redis_tag, "Client is done")
	}
	if that.conf.OnExit != nil {
		that.conf.OnExit(nil)
	}
}

func (that *RedisPubSub) Start() {
	if that.connected { // 已连接, 不再重连
		if that.logf != nil {
			that.logf(klog.InfoLevel, redis_tag, "Client is connected, do nothing")
		}
		return
	}

	subCtx := that.ctx.NewChild("redismq_pub")
	err := that.connectUtil(subCtx)
	if err != nil {
		if that.logf != nil {
			that.logf(klog.WarnLevel, redis_tag, "Connect error: {}", err)
		}
		if that.conf.OnError != nil {
			that.conf.OnError(err)
		}
		subCtx.Cancel()
		subCtx.Remove()
		return
	}

	if that.conf != nil && that.conf.OnReady != nil {
		that.conf.OnReady(true)
	}

END_LOOP:
	for {
		select {
		case <-that.ctx.Context().Done():
			{
				break END_LOOP
			}
		case msg := <-that.queue:
			{
				err := that.redisHandler.Publish(msg.Topic, msg.Message)
				if err != nil && that.logf != nil {
					that.logf(klog.WarnLevel, redis_tag, "Publish error: {}", err)
				}
			}
		}
	}

	subCtx.Cancel()
	subCtx.Remove()
	that.stop()
	if that.logf != nil {
		that.logf(klog.InfoLevel, redis_tag, "Client is done")
	}

	if that.conf.OnExit != nil {
		that.conf.OnExit(nil)
	}
}

func (that *RedisPubSub) receivedMessage(topic string, payload interface{}) (*kredis.RedisMessage, error) {
	s, ok := payload.(string)
	if ok {
		return &kredis.RedisMessage{Topic: topic, Message: s}, nil
	} else {
		return nil, kstrings.Errorf("payload data type unknown")
	}
}

func (that *RedisPubSub) Publish(msg *kredis.RedisMessage) bool {
	if that != nil && that.queue != nil {
		select {
		case that.queue <- msg:
			return true
		default:
		}
	}
	return false
}

func (that *RedisPubSub) PublishMessage(topic string, message string) bool {
	msg := &kredis.RedisMessage{
		Topic:   topic,
		Message: message,
	}
	return that.Publish(msg)
}

func (that *RedisPubSub) PublishData(topic string, message string) {
	msg := &kredis.RedisMessage{
		Topic:   topic,
		Message: message,
	}
	err := that.redisHandler.Publish(msg.Topic, msg.Message)
	if err != nil {
		if that.logf != nil {
			that.logf(klog.WarnLevel, redis_tag, "Publish error: {}", err)
		}

		if that.conf.OnError != nil {
			that.conf.OnError(err)
		}
	}
}

func (that *RedisPubSub) connectUtil(ctx *kcontext.ContextNode) error {
	retry := int(that.conf.Retry) //重连次数
	infinity := false
	if retry == -1 { // retry 为-1时, 无限重连
		retry = 1
		infinity = true
	}

	for i := 0; i < retry; i++ {
		if infinity { // retry值为-1,
			i--
		}
		select {
		case <-ctx.Context().Done():
			return kstrings.Errorf("client cancel db start")
		default:

		}

		if !that.dbConnect() {
			if that.logf != nil {
				that.logf(klog.WarnLevel, redis_tag, "connect to redis {}:{} - {} faulted, retry: {}", that.conf.Host, int(that.conf.Port), that.conf.DB, i)
			}
			time.Sleep(500 * time.Millisecond)
		} else {
			break
		}
	}
	if !that.connected {
		return kstrings.Errorf("connect to redis {}:{} - {} faulted", that.conf.Host, int(that.conf.Port), that.conf.DB)
	}
	return nil
}

func (that *RedisPubSub) dbConnect() bool {
	if nil == that.redisHandler {
		that.redisHandler = kredis.NewKRedis(that.ctx.Context(), that.conf.Host, int(that.conf.Port), "", that.conf.Password, int(that.conf.DB))
	}

	if !that.redisHandler.Ping() { //探测连接失败
		that.connected = false
		that.redisHandler.Client.Close()
		that.redisHandler.Stop()
		that.redisHandler = nil
	} else {
		that.connected = true
	}

	return that.connected
}

func (that *RedisPubSub) stop() {
	if that.connected {
		that.redisHandler.Stop()
		that.connected = false
		close(that.queue)
		if that.logf != nil {
			that.logf(klog.InfoLevel, redis_tag, "Client stopped")
		}
	}
}

func (that *RedisPubSub) Close() {
	that.ctx.Cancel()
	that.ctx.Remove()
}
