package source

import (
	"fmt"
	"time"

	"github.com/khan-lau/kmq-utils/kafkamq"
	"github.com/khan-lau/kmq/service/idl"
	"github.com/khan-lau/kmq/service/mq/config"
	"github.com/khan-lau/kutils/container/kcontext"
	klog "github.com/khan-lau/kutils/klogger"
)

const (
	KafkaSourceLogTag = "kafkamq_source"
)

type KafkaMQ struct {
	ctx               *kcontext.ContextNode
	conf              *config.KafkaConfig
	name              string // 服务名称
	kafkaBuffSize     uint   // 消息队列大小
	status            idl.ServiceStatus
	subscriber        *kafkamq.ConsumerGroup
	logf              klog.AppLogFuncWithTag
	OnRecivedCallback idl.OnRecived // 消息接收回调函数
}

func NewKafkaMQ(ctx *kcontext.ContextNode, name string, conf *config.KafkaConfig, kafkaBuffSize uint, logf klog.AppLogFuncWithTag) (*KafkaMQ, error) {
	subCtx := ctx.NewChild(fmt.Sprintf("%s_%s", KafkaSourceLogTag, name))

	rabbitMQ := &KafkaMQ{
		ctx:           subCtx,
		conf:          conf,
		name:          name,
		kafkaBuffSize: kafkaBuffSize,
		status:        idl.ServiceStatusStopped,
		subscriber:    nil,
		logf:          logf,
	}

	return rabbitMQ, nil
}

func (that *KafkaMQ) Name() string {
	return that.name
}

func (that *KafkaMQ) Init() {}

func (that *KafkaMQ) SetOnRecivedCallback(callback idl.OnRecived) {
	that.OnRecivedCallback = callback
}

func (that *KafkaMQ) StartAsync() {
	go func() {
		err := that.Start()
		if err != nil {
			that.log(klog.ErrorLevel, "start service %s error: %v", that.name, err)
			that.onError(that.name, err)
		}
	}()
}

func (that *KafkaMQ) Start() error {
	if that.status != idl.ServiceStatusStopped { //检查服务状态 是否为停止状态
		return fmt.Errorf("service %s is not stopped, status=%v", that.name, that.status)
	}

	netConfig := kafkamq.NewNetConfig().
		SetDialTimeout(time.Duration(that.conf.Net.DialTimeout) * time.Millisecond).
		SetMaxOpenRequests(that.conf.Net.MaxOpenRequests).
		SetReadTimeout(time.Duration(that.conf.Net.ReadTimeout) * time.Millisecond).
		SetWriteTimeout(time.Duration(that.conf.Net.WriteTimeout) * time.Millisecond).
		SetKeepAlive(time.Duration(that.conf.Net.KeepAlive) * time.Millisecond).
		SetResolveHost(that.conf.Net.ResolveHost)

	kafkaConsumerConfig := kafkamq.NewKafkaConsumerConfig().
		SetMaxProcessingTime(time.Duration(that.conf.Consumer.MaxProcessingTime)*time.Millisecond).
		SetSessionTimeout(time.Duration(that.conf.Consumer.SessionTimeout)*time.Millisecond).
		SetHeartbeatInterval(time.Duration(that.conf.Consumer.HeartbeatInterval)*time.Millisecond).
		SetInitialOffset(that.conf.Consumer.InitialOffset).
		SetRebalanceTimeout(time.Duration(that.conf.Consumer.RebalanceTimeout)*time.Millisecond).
		SetFetch(that.conf.Consumer.Min, that.conf.Consumer.Max, that.conf.Consumer.Fetch, time.Duration(that.conf.Consumer.MaxWaitTime)*time.Millisecond).
		SetAssignor(that.conf.Consumer.Assignor)

	switch that.conf.Consumer.AutoCommit {
	case kafkamq.AUTO_COMMIT_NATIVE:
		kafkaConsumerConfig.NativeAutoCommit(time.Duration(that.conf.Consumer.AutoCommitInterval) * time.Millisecond)
	// case kafkamq.AUTO_COMMIT_CUSTOM:
	// 	kafkaConsumerConfig.CustomAutoCommit()
	default:
		kafkaConsumerConfig.DisableAutoCommit()
	}

	// 载入上次消费的offset
	topics := make([]*kafkamq.Topic, 0, len(that.conf.Consumer.Topics))
	for _, item := range that.conf.Consumer.Topics {
		topic := kafkamq.NewTopic(item.Name)
		for _, partition := range item.Partitions {
			topic.SetOffset(int32(partition.Partition), partition.Offset)
		}
		topics = append(topics, topic)
	}

	kafkaConfig := kafkamq.NewKafkaConfig().
		SetVersion(that.conf.Version).
		SetClientID(that.conf.ClientID).
		SetGroupID(that.conf.GroupID).
		AddBrokers(that.conf.BrokerList...).
		AddTopic(topics...).
		SetChannelBufferSize(that.conf.ChannelBufferSize).
		SetNet(netConfig).
		SetConsumer(kafkaConsumerConfig)

	kafkaConfig.SetExitCallback(func(event any) { that.onExit(event) })
	kafkaConfig.SetErrorCallback(func(err error) { that.onError(that.name, err) })
	kafkaConfig.Consumer.SetMessageHandler(func(voidObj any, msg *kafkamq.KafkaMessage) {
		keyMap := map[string]string{"key": string(msg.Key)}
		that.OnRecved(msg, msg.Topic, int(msg.Partition), msg.Offset, keyMap, msg.Value)
	})

	subscriber, err := kafkamq.NewConsumerGroup(that.ctx, that.kafkaBuffSize, kafkaConfig, that.logf)
	if err != nil {
		return err
	}

	that.subscriber = subscriber
	go func() {
		// sleep 500ms, 等待服务启动完成
		time.Sleep(500 * time.Millisecond)
		that.status = idl.ServiceStatusRunning //设置服务状态为运行状态
	}()
	that.subscriber.SyncSubscribe()

	return nil
}

func (that *KafkaMQ) Restart() error {
	if that.status != idl.ServiceStatusRunning { //检查服务状态 是否为运行状态
		err := that.Stop()
		if err != nil {
			return err
		}
	}

	err := that.Start()
	return err
}

func (that *KafkaMQ) Stop() error {
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

func (that *KafkaMQ) onError(obj any, err error) {
}

func (that *KafkaMQ) onExit(obj any) {
}

func (that *KafkaMQ) OnRecved(origin any, topic string, partition int, offset int64, properties map[string]string, message []byte) {
	if that.OnRecivedCallback != nil {
		that.OnRecivedCallback(origin, that.Name(), topic, partition, offset, properties, message)
	}
}

// log 日志记录, 会自动添加 KafkaSourceLogTag
//
//go:inline
func (that *KafkaMQ) log(level klog.Level, format string, args ...any) {
	if that.logf != nil {
		that.logf(level, KafkaSourceLogTag, 1, format, args...)
	}
}
