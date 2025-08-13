package target

import (
	"time"

	"github.com/khan-lau/kmq/example/bean/config"
	"github.com/khan-lau/kmq/example/service/idl"
	"github.com/khan-lau/kmq/nats"

	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/container/kslices"
	"github.com/khan-lau/kutils/container/kstrings"
	klog "github.com/khan-lau/kutils/klogger"
)

const (
	nats_core_tag = "nats_core_target"
	nats_js_tag   = "nats_js_target"
)

type NatsCoreMQ struct {
	ctx       *kcontext.ContextNode
	conf      *config.NatsCoreConfig
	name      string // 服务名称
	status    idl.ServiceStatus
	publisher *nats.NatsCoreClient

	logf klog.AppLogFuncWithTag
}

func NewNatsCoreMQ(ctx *kcontext.ContextNode, name string, conf *config.NatsCoreConfig, logf klog.AppLogFuncWithTag) (*NatsCoreMQ, error) {
	subCtx := ctx.NewChild(kstrings.FormatString("{}_{}", nats_core_tag, name))

	{
		// 检查配置参数
		if conf.UseTls && (conf.TlsClientCert == "" || conf.KeyPath == "") {
			return nil, kstrings.Errorf("tls client cert or key path is empty")
		}

		if conf.UseTls && !kslices.Contains([]int{0x0300, 0x0301, 0x0302, 0x0303, 0x0304}, conf.MinTlsVer) {
			return nil, kstrings.Errorf("min tls version is not supported")
		}

		if len(conf.BrokerList) == 0 {
			return nil, kstrings.Errorf("broker list is empty")
		}

		if len(conf.Topics) == 0 {
			return nil, kstrings.Errorf("topics is empty")
		}
	}

	natsCoreMq := &NatsCoreMQ{
		ctx:       subCtx,
		conf:      conf,
		name:      name,
		status:    idl.ServiceStatusStopped,
		publisher: nil,
		logf:      logf,
	}

	return natsCoreMq, nil
}

func (that *NatsCoreMQ) Name() string {
	return that.name
}

func (that *NatsCoreMQ) Init() {}

func (that *NatsCoreMQ) StartAsync() {
	go func() {
		err := that.Start()
		if err != nil {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, nats_core_tag, "start service {} error: {}", that.name, err)
			}
			that.onError(that.name, err)
		}
	}()
}

func (that *NatsCoreMQ) Start() error {
	if that.status != idl.ServiceStatusStopped { //检查服务状态 是否为停止状态
		return kstrings.Errorf("service {} is not stopped, status={}", that.name, that.status)
	}

	natsConf := nats.NewNatsClientConfig().SetNats(nats.NewNatsConnConfig(that.conf.ClientID)).SetCoreNats(nats.NewCoreNatsConfig())
	natsConf.Nats().AddServers(that.conf.BrokerList...)

	natsConf.Nats().SetPing(that.conf.PingInterval, that.conf.MaxPingsOut)
	natsConf.Nats().SetUserPassword(that.conf.User, that.conf.Password)

	if that.conf.AllowReconnect {
		natsConf.Nats().EnableReconnect(that.conf.MaxReconnect, that.conf.ReconnectBufSize, that.conf.ConnectTimeout, that.conf.ReconnectWait)
	} else {
		natsConf.Nats().DisableReconnect()
	}
	if that.conf.UseTls {
		natsConf.Nats().EnableTls(that.conf.CaCertPath, that.conf.TlsClientCert, that.conf.KeyPath, that.conf.InsecureSkipVerify, that.conf.MinTlsVer)
	} else {
		natsConf.Nats().DisableTls()
	}

	natsConf.CoreNats().AddTopics(that.conf.Topics...).SetQueueGroup(that.conf.QueueGroup).SetMaxPending(that.conf.MaxPending)

	publisher, err := nats.NewNatsCoreClient(that.ctx, 20000, natsConf, that.logf)
	if err != nil {
		return err
	}
	that.publisher = publisher

	natsConf.SetOnExit(func(event interface{}) {
		that.onExit(event)
	})

	natsConf.SetOnError(func(err error) {
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
	if that.publisher != nil {
		that.publisher.Close()
	}
	that.status = idl.ServiceStatusStopped // 设置服务状态为停止状态
	that.publisher = nil
	time.Sleep(500 * time.Millisecond)
	that.ctx.Remove()
	return nil
}

func (that *NatsCoreMQ) Broadcast(message []byte, properties map[string]string) bool {
	for _, topic := range that.conf.Topics {
		if !that.PublishMessage(topic, string(message)) {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, nats_core_tag, "publish topic {} message {} fault", topic, string(message))
			}
		}
	}
	return true
}

func (that *NatsCoreMQ) Publish(topic string, message []byte, _ map[string]string) bool {
	return that.PublishMessage(topic, string(message))
}

func (that *NatsCoreMQ) PublishMessage(topic string, message string) bool {
	if that.status != idl.ServiceStatusRunning {
		return false
	}
	return that.publisher.PublishMessage(topic, message)
}

func (that *NatsCoreMQ) onError(obj interface{}, err error) {
}

func (that *NatsCoreMQ) onExit(obj interface{}) {
}

///////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////

type NatsJetStreamMQ struct {
	ctx       *kcontext.ContextNode
	conf      *config.NatsJsConfig
	name      string // 服务名称
	status    idl.ServiceStatus
	publisher *nats.NatsJetStreamClient

	logf klog.AppLogFuncWithTag
}

func NewNatsJetStreamMQ(ctx *kcontext.ContextNode, name string, conf *config.NatsJsConfig, logf klog.AppLogFuncWithTag) (*NatsJetStreamMQ, error) {
	subCtx := ctx.NewChild(kstrings.FormatString("{}_{}", nats_core_tag, name))

	{
		// 检查配置参数
		if conf.UseTls && (conf.TlsClientCert == "" || conf.KeyPath == "") {
			return nil, kstrings.Errorf("tls client cert or key path is empty")
		}

		if conf.UseTls && !kslices.Contains([]int{0x0300, 0x0301, 0x0302, 0x0303, 0x0304}, conf.MinTlsVer) {
			return nil, kstrings.Errorf("min tls version is not supported")
		}

		if len(conf.BrokerList) == 0 {
			return nil, kstrings.Errorf("broker list is empty")
		}

		if len(conf.Topics) == 0 {
			return nil, kstrings.Errorf("topics is empty")
		}

		if len(conf.QueueName) == 0 {
			return nil, kstrings.Errorf("queue name is empty")
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

		// if conf.ConsumerConfig == nil {
		// 	return nil, kstrings.Errorf("consumer config is empty")
		// }

		// if conf.ConsumerConfig != nil {
		// 	// 默认应答策略为none
		// 	if !kslices.Contains([]string{"none", "all", "explicit"}, conf.ConsumerConfig.AckPolicy) {
		// 		conf.ConsumerConfig.AckPolicy = "none"
		// 	}

		// 	// 默认投递策略为by_start_time
		// 	// if !kslices.Contains([]string{"all", "last", "new", "by_start_sequence", "by_start_time", "last_per_subject"}, conf.ConsumerConfig.DeliverPolicy) {
		// 	if !kslices.Contains([]string{"by_start_time"}, conf.ConsumerConfig.DeliverPolicy) {
		// 		conf.ConsumerConfig.DeliverPolicy = "by_start_time"
		// 	}
		// }
	}

	natsJSMq := &NatsJetStreamMQ{
		ctx:       subCtx,
		conf:      conf,
		name:      name,
		status:    idl.ServiceStatusStopped,
		publisher: nil,
		logf:      logf,
	}

	return natsJSMq, nil
}

func (that *NatsJetStreamMQ) Name() string {
	return that.name
}

func (that *NatsJetStreamMQ) Init() {}

func (that *NatsJetStreamMQ) StartAsync() {
	go func() {
		err := that.Start()
		if err != nil {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, nats_js_tag, "start service {} error: {}", that.name, err)
			}
			that.onError(that.name, err)
		}
	}()
}

func (that *NatsJetStreamMQ) Start() error {
	if that.status != idl.ServiceStatusStopped { //检查服务状态 是否为停止状态
		return kstrings.Errorf("service {} is not stopped, status={}", that.name, that.status)
	}

	// nats连接配置
	natsConf := nats.NewNatsClientConfig().SetNats(nats.NewNatsConnConfig(that.conf.ClientID)).SetJetStream(nats.NewJetStreamConfig(that.conf.QueueName))
	natsConf.Nats().AddServers(that.conf.BrokerList...)

	natsConf.Nats().SetPing(that.conf.PingInterval, that.conf.MaxPingsOut)
	natsConf.Nats().SetUserPassword(that.conf.User, that.conf.Password)

	if that.conf.AllowReconnect {
		natsConf.Nats().EnableReconnect(that.conf.MaxReconnect, that.conf.ReconnectBufSize, that.conf.ConnectTimeout, that.conf.ReconnectWait)
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
	jsConf.SetStorageType(jsConf.StorageTypeFromStr(that.conf.StorageType))
	jsConf.SetCompression(jsConf.StorageCompressionFromStr(that.conf.StorageCompression))
	jsConf.SetDiscard(jsConf.DiscardFromStr(that.conf.Discard))
	jsConf.SetMaxConsumers(that.conf.MaxConsumers).SetRetentionLimits(that.conf.MaxMsgs, that.conf.MaxBytes, that.conf.MaxAge, that.conf.MaxMsgsPerSubject)
	jsConf.SetRetentionPolicy(jsConf.RetentionPolicyFromStr(that.conf.RetentionPolicy))
	jsConf.SetMaxMsgSize(that.conf.MaxMsgSize).SetDuplicates(that.conf.Duplicates)
	jsConf.AddTopic(that.conf.Topics...)

	// // 生产者封装, 无需消费组配置
	// jsConf.SetConsumer(nats.NewJetStreamConsumerConfig(that.conf.ConsumerConfig.GroupId))
	// consumerConf := jsConf.Consumer()
	// consumerConf.SetMaxWait(that.conf.ConsumerConfig.MaxWait)
	// consumerConf.SetAckPolicy(consumerConf.AckPolicyFromStr(that.conf.ConsumerConfig.AckPolicy))
	// consumerConf.SetDeliverPolicy(consumerConf.DeliverPolicyFromStr(that.conf.ConsumerConfig.DeliverPolicy))

	publisher, err := nats.NewNatsJetStreamClient(that.ctx, 20000, -1, natsConf, that.logf)
	if err != nil {
		return err
	}
	that.publisher = publisher

	natsConf.SetOnExit(func(event interface{}) {
		that.onExit(event)
	})

	natsConf.SetOnError(func(err error) {
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
	if that.publisher != nil {
		that.publisher.Close()
	}

	that.status = idl.ServiceStatusStopped // 设置服务状态为停止状态
	that.publisher = nil
	time.Sleep(500 * time.Millisecond)
	that.ctx.Remove()
	return nil
}

func (that *NatsJetStreamMQ) Broadcast(message []byte, properties map[string]string) bool {
	for _, topic := range that.conf.Topics {
		if !that.PublishMessage(topic, string(message)) {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, nats_js_tag, "publish topic {} message {} fault", topic, string(message))
			}
		}
	}
	return true
}

func (that *NatsJetStreamMQ) Publish(topic string, message []byte, _ map[string]string) bool {
	return that.PublishMessage(topic, string(message))
}

func (that *NatsJetStreamMQ) PublishMessage(topic string, message string) bool {
	if that.status != idl.ServiceStatusRunning {
		return false
	}
	return that.publisher.PublishMessage(topic, message)
}

func (that *NatsJetStreamMQ) onError(obj interface{}, err error) {
}

func (that *NatsJetStreamMQ) onExit(obj interface{}) {
}
