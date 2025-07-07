package target

import (
	"time"

	"github.com/khan-lau/kmq/example/bean/config"
	"github.com/khan-lau/kmq/example/service/idl"
	"github.com/khan-lau/kmq/mqtt"
	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/container/kstrings"
	klog "github.com/khan-lau/kutils/klogger"
)

const (
	mqtt_tag = "redismq_target"
)

type MqttMQ struct {
	ctx       *kcontext.ContextNode
	conf      *config.MqttConfig
	name      string // 服务名称
	status    idl.ServiceStatus
	publisher *mqtt.MqttSubPub

	logf klog.AppLogFuncWithTag
}

func NewMqttMQ(ctx *kcontext.ContextNode, name string, conf *config.MqttConfig, logf klog.AppLogFuncWithTag) (*MqttMQ, error) {
	subCtx := ctx.NewChild(kstrings.FormatString("{}_{}", redismq_tag, name))

	redisMQ := &MqttMQ{
		ctx:       subCtx,
		conf:      conf,
		name:      name,
		status:    idl.ServiceStatusStopped,
		publisher: nil,
		logf:      logf,
	}

	return redisMQ, nil
}

func (that *MqttMQ) Name() string {
	return that.name
}

func (that *MqttMQ) Init() {}

func (that *MqttMQ) StartAsync() {
	go func() {
		err := that.Start()
		if err != nil {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, mqtt_tag, "start service {} error: {}", that.name, err)
			}
			that.onError(that.name, err)
		}
	}()
}

func (that *MqttMQ) Start() error {
	if that.status != idl.ServiceStatusStopped { //检查服务状态 是否为停止状态
		return kstrings.Errorf("service {} is not stopped, status={}", that.name, that.status)
	}

	mqttConf := mqtt.New().
		AddBorker(that.conf.Broker).
		SetClientId(that.conf.ClientID).
		SetUsername(that.conf.UserName).SetPassword(that.conf.Password).
		SetKeepAlive(int32(that.conf.KeepAlive)).
		SetCleanSession(that.conf.CleanSession).
		SetQos(byte(that.conf.Qos)).
		SetVersion(that.conf.Version).SetTimeout(that.conf.Timeout).
		SetWillTopic(that.conf.WillTopic).SetWillQos(byte(that.conf.WillQos)).SetWillRetain(that.conf.WillRetain).SetWillPayload([]byte(that.conf.WillPayload)).
		SetTopics(that.conf.Topics...).SetUseTLS(that.conf.UseTLS).SetCaCertPath(that.conf.CaCertPath)

	publisher, err := mqtt.NewMQTTClient(that.ctx, 20000, mqttConf, that.logf)
	if err != nil {
		return err
	}
	mqttConf.SetOnAuthedCallback(func(client *mqtt.MqttSubPub, isAuthed bool) {
		if isAuthed {
			// 连接且鉴权成功, 开始发送循环
			that.publisher.ReadySend()
		} else {
			that.onExit(client)
		}
	})
	that.publisher = publisher

	// sleep 500ms, 等待服务启动完成
	time.Sleep(500 * time.Millisecond)
	that.status = idl.ServiceStatusRunning //设置服务状态为运行状态
	flag := that.publisher.SyncStart()
	if !flag {
		return kstrings.Errorf("service {} start failed", that.name)
	}
	return nil
}

func (that *MqttMQ) Restart() error {
	if that.status != idl.ServiceStatusRunning { //检查服务状态 是否为运行状态
		err := that.Stop()
		if err != nil {
			return err
		}
	}

	err := that.Start()
	return err
}

func (that *MqttMQ) Stop() error {
	that.ctx.Cancel()
	that.publisher.Close()
	that.status = idl.ServiceStatusStopped // 设置服务状态为停止状态
	that.publisher = nil
	time.Sleep(500 * time.Millisecond)
	that.ctx.Remove()
	return nil
}

func (that *MqttMQ) onError(obj interface{}, err error) {
}

func (that *MqttMQ) onExit(obj interface{}) {
}

func (that *MqttMQ) Broadcast(message []byte, properties map[string]string) bool {
	for _, topic := range that.conf.Topics {
		if !that.PublishMessage(topic, string(message)) {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, mqtt_tag, "publish topic {} message {} fault", topic, string(message))
			}
		}
	}
	return true
}

func (that *MqttMQ) Publish(topic string, message []byte, _ map[string]string) bool {
	return that.PublishMessage(topic, string(message))
}

func (that *MqttMQ) PublishMessage(topic string, message string) bool {
	if that.status != idl.ServiceStatusRunning {
		return false
	}
	return that.publisher.PublishMessage(topic, message)
}
