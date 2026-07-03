package source

import (
	"fmt"
	"time"

	"github.com/khan-lau/kmq-utils/mqtt"
	"github.com/khan-lau/kmq/service/idl"
	"github.com/khan-lau/kmq/service/mq/config"
	"github.com/khan-lau/kutils/container/kcontext"
	klog "github.com/khan-lau/kutils/klogger"
)

const (
	MqttSourceLogTag = "mqtt_source"
)

type MqttMQ struct {
	ctx               *kcontext.ContextNode
	conf              *config.MqttConfig
	name              string // 服务名称
	mqttBuffSize      uint
	status            idl.ServiceStatus
	subscriber        *mqtt.MqttClient
	logf              klog.AppLogFuncWithTag
	OnRecivedCallback idl.OnRecived // 消息接收回调函数
}

func NewMqttMQ(ctx *kcontext.ContextNode, name string, conf *config.MqttConfig, mqttBuffSize uint, logf klog.AppLogFuncWithTag) (*MqttMQ, error) {
	subCtx := ctx.NewChild(fmt.Sprintf("%s_%s", MqttSourceLogTag, name))

	mqttMQ := &MqttMQ{
		ctx:          subCtx,
		conf:         conf,
		name:         name,
		mqttBuffSize: mqttBuffSize,
		status:       idl.ServiceStatusStopped,
		subscriber:   nil,
		logf:         logf,
	}

	return mqttMQ, nil
}

func (that *MqttMQ) Name() string {
	return that.name
}

func (that *MqttMQ) Init() {}

func (that *MqttMQ) SetOnRecivedCallback(callback idl.OnRecived) {
	that.OnRecivedCallback = callback
}

func (that *MqttMQ) StartAsync() {
	go func() {
		err := that.Start()
		if err != nil {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, MqttSourceLogTag, "start service %s error: %v", that.name, err)
			}
			that.onError(that.name, err)
		}
	}()
}

func (that *MqttMQ) Start() error {
	if that.status != idl.ServiceStatusStopped { //检查服务状态 是否为停止状态
		return fmt.Errorf("service %s is not stopped, status=%v", that.name, that.status)
	}

	mqttConf := mqtt.New().
		AddBroker(that.conf.Broker).
		SetClientId(that.conf.ClientID).
		SetUsername(that.conf.UserName).SetPassword(that.conf.Password).
		SetKeepAlive((time.Duration(that.conf.KeepAlive) * time.Millisecond)).
		SetCleanSession(that.conf.CleanSession).
		SetQos(byte(that.conf.Qos)).
		SetVersion(that.conf.Version).SetTimeout((time.Duration(that.conf.Timeout) * time.Millisecond)).
		SetWillTopic(that.conf.WillTopic).SetWillQos(byte(that.conf.WillQos)).SetWillRetain(that.conf.WillRetain).SetWillPayload([]byte(that.conf.WillPayload)).
		SetTopics(that.conf.Topics...).SetUseTLS(that.conf.UseTLS).SetCaCertPath(that.conf.CaCertPath)

	mqttConf.SetMessageHandler(func(voidObj any, msg *mqtt.MqttMessage) {
		that.OnRecved(nil, msg.Topic, 0, int64(msg.MessageID), nil, msg.Payload)
	})

	subscriber, err := mqtt.NewMqttClient(that.ctx, that.mqttBuffSize, mqttConf, that.logf)
	if err != nil {
		return err
	}
	// mqttConf.SetOnAuthedCallback(func(client *mqtt.MqttClient, isAuthed bool) {
	// 	if isAuthed {
	// 		// 断开重连, 并且鉴权成功后, 需要重新订阅主题
	// 		client.Subscribe(nil, func(voidObj any, msg *mqtt.MqttMessage) {
	// 			that.OnRecved(nil, msg.Topic, 0, int64(msg.MessageID), nil, msg.Payload)
	// 		})
	// 	} else {
	// 		that.onExit(client)
	// 	}
	// })
	that.subscriber = subscriber

	// sleep 500ms, 等待服务启动完成
	time.Sleep(500 * time.Millisecond)
	that.status = idl.ServiceStatusRunning //设置服务状态为运行状态
	err = that.subscriber.Start()
	if err != nil {
		return fmt.Errorf("service %s start failed", that.name)
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
	if that.subscriber != nil {
		that.subscriber.Close()
	}
	that.status = idl.ServiceStatusStopped // 设置服务状态为停止状态
	that.subscriber = nil
	time.Sleep(500 * time.Millisecond)
	that.ctx.Remove()
	return nil
}

func (that *MqttMQ) onError(obj any, err error) {
}

// func (that *MqttMQ) onExit(obj any) {
// }

func (that *MqttMQ) OnRecved(origin any, topic string, partition int, offset int64, properties map[string]string, message []byte) {
	if that.OnRecivedCallback != nil {
		that.OnRecivedCallback(origin, that.Name(), topic, partition, offset, properties, message)
	}
}

// func (that *MqttMQ) UnSubscribe(topics ...string) bool {
// 	if that.status != idl.ServiceStatusRunning {
// 		return false
// 	}
// 	return that.subscriber.UnSubscribe(topics...)
// }
