package source

import (
	"fmt"
	"time"

	"github.com/khan-lau/kmq-utils/natsmq"
	"github.com/khan-lau/kmq/service/idl"
	"github.com/khan-lau/kmq/service/mq/config"
	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/container/kslices"
	"github.com/khan-lau/kutils/expr/condexpr"
	klog "github.com/khan-lau/kutils/klogger"
)

const (
	nats_core_tag = "nats_core_source"
	nats_js_tag   = "nats_js_source"
)

type NatsCoreMQ struct {
	ctx          *kcontext.ContextNode
	conf         *config.NatsCoreConfig
	name         string // 服务名称
	coreBuffSize uint
	status       idl.ServiceStatus
	subscriber   *natsmq.NatsCoreClient

	logf              klog.AppLogFuncWithTag
	OnRecivedCallback idl.OnRecived // 消息接收回调函数
}

func NewNatsCoreMQ(ctx *kcontext.ContextNode, name string, conf *config.NatsCoreConfig, coreBuffSize uint, logf klog.AppLogFuncWithTag) (*NatsCoreMQ, error) {
	subCtx := ctx.NewChild(fmt.Sprintf("%s_%s", nats_core_tag, name))

	{
		// 检查配置参数
		if conf.UseTls && (conf.TlsClientCert == "" || conf.KeyPath == "") {
			return nil, fmt.Errorf("tls client cert or key path is empty")
		}

		if conf.UseTls && !kslices.Contains([]int{0x0300, 0x0301, 0x0302, 0x0303, 0x0304}, conf.MinTlsVer) {
			return nil, fmt.Errorf("min tls version is not supported")
		}

		if len(conf.BrokerList) == 0 {
			return nil, fmt.Errorf("broker list is empty")
		}

		if len(conf.Topics) == 0 {
			return nil, fmt.Errorf("topics is empty")
		}

	}

	natsCoreMq := &NatsCoreMQ{
		ctx:          subCtx,
		conf:         conf,
		name:         name,
		coreBuffSize: coreBuffSize,
		status:       idl.ServiceStatusStopped,
		subscriber:   nil,
		logf:         logf,
	}

	return natsCoreMq, nil
}

func (that *NatsCoreMQ) Name() string {
	return that.name
}

func (that *NatsCoreMQ) Init() {}

func (that *NatsCoreMQ) SetOnRecivedCallback(callback idl.OnRecived) {
	that.OnRecivedCallback = callback
}

func (that *NatsCoreMQ) StartAsync() {
	go func() {
		err := that.Start()
		if err != nil {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, nats_core_tag, "start service %s error: %v", that.name, err)
			}
			that.onError(that.name, err)
		}
	}()
}

func (that *NatsCoreMQ) Start() error {
	if that.status != idl.ServiceStatusStopped { //检查服务状态 是否为停止状态
		return fmt.Errorf("service %s is not stopped, status=%v", that.name, that.status)
	}

	natsConf := natsmq.NewNatsClientConfig().SetNats(natsmq.NewNatsConnConfig(that.conf.ClientID)).SetCoreNats(natsmq.NewCoreNatsConfig())
	natsConf.Nats().AddServers(that.conf.BrokerList...)

	natsConf.Nats().SetPing((time.Duration(that.conf.PingInterval) * time.Millisecond), that.conf.MaxPingsOut)
	natsConf.Nats().SetUserPassword(that.conf.User, that.conf.Password)

	if that.conf.AllowReconnect {
		natsConf.Nats().EnableReconnect(that.conf.MaxReconnect, that.conf.ReconnectBufSize,
			(time.Duration(that.conf.ConnectTimeout) * time.Millisecond), (time.Duration(that.conf.ReconnectWait) * time.Millisecond))
	} else {
		natsConf.Nats().DisableReconnect()
	}
	if that.conf.UseTls {
		natsConf.Nats().EnableTls(that.conf.CaCertPath, that.conf.TlsClientCert, that.conf.KeyPath, that.conf.InsecureSkipVerify, that.conf.MinTlsVer)
	} else {
		natsConf.Nats().DisableTls()
	}

	natsConf.CoreNats().AddTopics(that.conf.Topics...).SetQueueGroup(that.conf.QueueGroup).SetMaxPending(that.conf.MaxPending)

	subscriber, err := natsmq.NewNatsCoreClient(that.ctx, that.coreBuffSize, natsConf, that.logf)
	if err != nil {
		return err
	}
	that.subscriber = subscriber

	natsConf.SetOnExit(func(event any) {
		that.onExit(event)
	})

	natsConf.SetOnError(func(err error) {
		that.onError(that.name, err)
	})

	natsConf.CoreNats().SetMainHandler(func(voidObj any, msg *natsmq.NatsMessage) {
		that.OnRecved(msg, msg.Topic, 0, msg.Seq, nil, []byte(msg.Payload))
	})

	go func() {
		// sleep 500ms, 等待服务启动完成
		time.Sleep(500 * time.Millisecond)
		that.status = idl.ServiceStatusRunning //设置服务状态为运行状态
	}()

	err = that.subscriber.Start()

	return err
}

func (that *NatsCoreMQ) Restart() error {
	if that.status != idl.ServiceStatusRunning { //检查服务状态 是否为运行状态
		err := that.Stop()
		if err != nil {
			return err
		}
	}

	err := that.Start()
	return err
}

func (that *NatsCoreMQ) Stop() error {
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

func (that *NatsCoreMQ) onError(obj any, err error) {
}

func (that *NatsCoreMQ) onExit(obj any) {
}

func (that *NatsCoreMQ) OnRecved(origin any, topic string, partition int, offset int64, properties map[string]string, message []byte) {
	if that.OnRecivedCallback != nil {
		that.OnRecivedCallback(origin, that.Name(), topic, partition, offset, properties, message)
	}
}

///////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////

type NatsJetStreamMQ struct {
	ctx        *kcontext.ContextNode
	conf       *config.NatsJsConfig
	name       string // 服务名称
	jsBuffSize uint
	status     idl.ServiceStatus
	subscriber *natsmq.NatsJetStreamClient

	logf              klog.AppLogFuncWithTag
	OnRecivedCallback idl.OnRecived // 消息接收回调函数
}

func NewNatsJetStreamMQ(ctx *kcontext.ContextNode, name string, conf *config.NatsJsConfig, jsBuffSize uint, logf klog.AppLogFuncWithTag) (*NatsJetStreamMQ, error) {
	subCtx := ctx.NewChild(fmt.Sprintf("%s_%s", nats_js_tag, name))

	{
		// 检查配置参数
		if conf.UseTls && (conf.TlsClientCert == "" || conf.KeyPath == "") {
			return nil, fmt.Errorf("tls client cert or key path is empty")
		}

		if conf.UseTls && !kslices.Contains([]int{0x0300, 0x0301, 0x0302, 0x0303, 0x0304}, conf.MinTlsVer) {
			return nil, fmt.Errorf("min tls version is not supported")
		}

		if len(conf.BrokerList) == 0 {
			return nil, fmt.Errorf("broker list is empty")
		}

		if len(conf.Topics) == 0 {
			return nil, fmt.Errorf("topics is empty")
		}

		// 默认内存存储
		if !kslices.Contains([]string{"memory", "file"}, conf.StorageType) {
			conf.StorageType = "memory"
		}
		// 默认不压缩
		if !kslices.Contains([]string{"none", "s2"}, conf.StorageCompression) {
			conf.StorageCompression = "none"
		}
		// 默认保留策略为limits
		if !kslices.Contains([]string{"limits", "interest", "workqueue"}, conf.RetentionPolicy) {
			conf.RetentionPolicy = "limits"
		}
		// 默认丢弃策略为old
		if !kslices.Contains([]string{"old", "new"}, conf.Discard) {
			conf.Discard = "old"
		}

		if conf.ConsumerConfig == nil {
			return nil, fmt.Errorf("consumer config is empty")
		}

		// 默认应答策略为none
		if !kslices.Contains([]string{"none", "all", "explicit"}, conf.ConsumerConfig.AckPolicy) {
			conf.ConsumerConfig.AckPolicy = "none"
		}

		// 默认投递策略为by_start_time
		// if !kslices.Contains([]string{"all", "last", "new", "by_start_sequence", "by_start_time", "last_per_subject"}, conf.ConsumerConfig.DeliverPolicy) {
		if !kslices.Contains([]string{"by_start_time"}, conf.ConsumerConfig.DeliverPolicy) {
			conf.ConsumerConfig.DeliverPolicy = "by_start_time"
		}
	}

	natsJSMq := &NatsJetStreamMQ{
		ctx:        subCtx,
		conf:       conf,
		name:       name,
		jsBuffSize: jsBuffSize,
		status:     idl.ServiceStatusStopped,
		subscriber: nil,
		logf:       logf,
	}

	return natsJSMq, nil
}

func (that *NatsJetStreamMQ) Name() string {
	return that.name
}

func (that *NatsJetStreamMQ) Init() {}

func (that *NatsJetStreamMQ) SetOnRecivedCallback(callback idl.OnRecived) {
	that.OnRecivedCallback = callback
}

func (that *NatsJetStreamMQ) StartAsync() {
	go func() {
		err := that.Start()
		if err != nil {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, nats_js_tag, "start service %s error: %v", that.name, err)
			}
			that.onError(that.name, err)
		}
	}()
}

func (that *NatsJetStreamMQ) Start() error {
	if that.status != idl.ServiceStatusStopped { //检查服务状态 是否为停止状态
		return fmt.Errorf("service %s is not stopped, status=%v", that.name, that.status)
	}

	// nats连接配置
	natsConf := natsmq.NewNatsClientConfig().SetNats(natsmq.NewNatsConnConfig(that.conf.ClientID)).SetJetStream(natsmq.NewJetStreamConfig(that.conf.QueueName))
	natsConf.Nats().AddServers(that.conf.BrokerList...).
		SetPing((time.Duration(that.conf.PingInterval)*time.Millisecond), that.conf.MaxPingsOut).
		SetUserPassword(that.conf.User, that.conf.Password)

	if that.conf.AllowReconnect {
		natsConf.Nats().EnableReconnect(that.conf.MaxReconnect, that.conf.ReconnectBufSize,
			(time.Duration(that.conf.ConnectTimeout) * time.Millisecond),
			(time.Duration(that.conf.ReconnectWait) * time.Millisecond))
	} else {
		natsConf.Nats().DisableReconnect()
	}
	if that.conf.UseTls {
		natsConf.Nats().EnableTls(that.conf.CaCertPath, that.conf.TlsClientCert, that.conf.KeyPath, that.conf.InsecureSkipVerify, that.conf.MinTlsVer)
	} else {
		natsConf.Nats().DisableTls()
	}

	// jetstream配置
	jsConf := natsConf.JetStream()
	jsConf.SetStorageType(jsConf.StorageTypeFromStr(that.conf.StorageType)).
		SetCompression(jsConf.StorageCompressionFromStr(that.conf.StorageCompression)).
		SetDiscard(jsConf.DiscardFromStr(that.conf.Discard)).
		SetMaxConsumers(that.conf.MaxConsumers).
		SetRetentionLimits(that.conf.MaxMsgs, that.conf.MaxBytes, int64(time.Duration(that.conf.MaxAge)*time.Millisecond), that.conf.MaxMsgsPerSubject).
		SetRetentionPolicy(jsConf.RetentionPolicyFromStr(that.conf.RetentionPolicy)).
		SetMaxMsgSize(that.conf.MaxMsgSize).SetDuplicates(int64(time.Duration(that.conf.Duplicates) * time.Millisecond)).
		AddTopic(that.conf.Topics...).
		// 消费者配置
		SetConsumer(natsmq.NewJetStreamConsumerConfig(that.conf.ConsumerConfig.GroupId))

	consumerConf := jsConf.Consumer()
	consumerConf.SetMaxWait(that.conf.ConsumerConfig.MaxWait).
		SetAckPolicy(consumerConf.AckPolicyFromStr(that.conf.ConsumerConfig.AckPolicy)).
		SetDeliverPolicy(consumerConf.DeliverPolicyFromStr(that.conf.ConsumerConfig.DeliverPolicy)).
		SetAutoCommit(that.conf.ConsumerConfig.AutoCommit)

	// 设置消费offset 时间戳
	startWithTimestamp := condexpr.CondExpr(that.conf.ConsumerConfig.StartWithTimestamp > -1, that.conf.ConsumerConfig.StartWithTimestamp, -1)
	subscriber, err := natsmq.NewNatsJetStreamClient(that.ctx, that.jsBuffSize, startWithTimestamp, natsConf, that.logf)
	if err != nil {
		return err
	}
	that.subscriber = subscriber

	natsConf.SetOnExit(func(event any) {
		that.onExit(event)
	})

	natsConf.SetOnError(func(err error) {
		that.onError(that.name, err)
	})

	natsConf.JetStream().SetMainHandler(func(voidObj any, msg *natsmq.NatsMessage) {
		that.OnRecved(msg, msg.Topic, 0, msg.Seq, nil, []byte(msg.Payload))
	})

	go func() {
		// sleep 500ms, 等待服务启动完成
		time.Sleep(500 * time.Millisecond)
		that.status = idl.ServiceStatusRunning //设置服务状态为运行状态
	}()
	err = that.subscriber.Start()

	return err
}

func (that *NatsJetStreamMQ) Restart() error {
	if that.status != idl.ServiceStatusRunning { //检查服务状态 是否为运行状态
		err := that.Stop()
		if err != nil {
			return err
		}
	}

	err := that.Start()
	return err
}

func (that *NatsJetStreamMQ) Stop() error {
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

func (that *NatsJetStreamMQ) onError(obj any, err error) {
}

func (that *NatsJetStreamMQ) onExit(obj any) {
}

func (that *NatsJetStreamMQ) OnRecved(origin any, topic string, partition int, offset int64, properties map[string]string, message []byte) {
	if that.OnRecivedCallback != nil {
		that.OnRecivedCallback(origin, that.Name(), topic, partition, offset, properties, message)
	}
}
