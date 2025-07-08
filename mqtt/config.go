package mqtt

import (
	"strings"

	"github.com/khan-lau/kutils/container/klists"
	"github.com/khan-lau/kutils/container/kstrings"
)

const (
	mqtt_tag = "mqtt"
)

type OnAuthedCallback func(client *MqttSubPub, isAuthed bool)

type MqttMessage struct {
	Topic     string
	Duplicate bool // 是否为重传消息, QOS1 QOS2 时使用，默认为false, paho.mqtt.golang 会自行管理, 无需手动设置
	Qos       byte // 质量标记, 0表示最多一次，1表示至少一次，2表示恰好一次
	Retained  bool // 遗嘱标记, 遗嘱消息是否保留，如果为true，则服务器会将此消息保存到持久存储中
	MessageID uint16
	Payload   []byte
}

func NewMqttMessage(topic string, duplicate bool, qos byte, retained bool, messageId uint16, payload []byte) *MqttMessage {
	return &MqttMessage{
		Topic:     topic,
		Duplicate: duplicate,
		Qos:       qos,
		Retained:  retained,
		MessageID: messageId,
		Payload:   payload,
	}
}

func (that *MqttMessage) String() string {
	return kstrings.FormatString("Topic: {}, Duplicate: {}, Qos: {}, Retained: {}, MessageID: {}, Payload: {}", that.Topic, that.Duplicate, that.Qos, that.Retained, that.MessageID, string(that.Payload))
}

////////////////////////////////////////////////////////////////////////////////////////////////////

type Config struct {
	broker       string // Broker 地址，例如 "tcp://127.0.0.1:1883"
	clientId     string // 客户端ID，用于唯一标识一个MQTT连接
	username     string // 用户名，用于连接MQTT服务器时进行身份验证
	password     string // 密码，用于连接MQTT服务器时进行身份验证
	keepAlive    int32  // 心跳间隔，单位为毫秒。客户端和服务器之间保持连接的心跳时间
	cleanSession bool   // 是否清除会话，如果为true，则断开连接后之前的订阅和消息都会被清空
	qos          byte   // 消息服务质量等级，0表示最多一次，1表示至少一次，2表示恰好一次
	willTopic    string // 遗嘱消息的主题，当客户端意外断开连接时，服务器会发布此主题的消息
	willPayload  []byte // 遗嘱消息的内容，当客户端意外断开连接时，服务器会发布此内容的消息
	willQos      byte   // 遗嘱消息的服务质量等级，0表示最多一次，1表示至少一次，2表示恰好一次
	willRetain   bool   // 遗嘱消息是否保留，如果为true，则服务器会将此消息保存到持久存储中

	version int                   // 协议版本 3: 3.1; 4: 3.1.1; 5: 5.0
	timeout int32                 // 通信超时时间，单位为毫秒
	topics  *klists.KList[string] // 订阅的主题列表

	useTLS           bool             // 是否启用 TLS
	caCertPath       string           // CA 证书路径, 仅当 useTLS 为 true 时有效
	onAuthedCallback OnAuthedCallback // 认证回调

}

func New() *Config {
	return &Config{
		topics: klists.New[string](),
	}
}

func NewMqttConfig(broker string, clientId string, username string, password string, keepAlive int32, cleanSession bool, qos byte,
	willTopic string, willPayload []byte, willQos byte, willRetain bool,
	version int,
	timeout int32,
	topics *klists.KList[string],
	useTLS bool,
	caCertPath string,
) *Config {

	return &Config{
		broker:       broker,
		clientId:     clientId,
		username:     username,
		password:     password,
		keepAlive:    keepAlive,
		cleanSession: cleanSession,
		qos:          qos,
		willTopic:    willTopic,
		willPayload:  willPayload,
		willQos:      willQos,
		willRetain:   willRetain,
		version:      version,
		timeout:      timeout,
		topics:       topics,
		useTLS:       useTLS,
		caCertPath:   caCertPath,
	}
}

func (that *Config) AddBorker(broker string) *Config {
	that.broker = broker
	return that
}

func (that *Config) SetClientId(clientId string) *Config {
	that.clientId = clientId
	return that
}

func (that *Config) SetUsername(username string) *Config {
	that.username = username
	return that
}

func (that *Config) SetPassword(password string) *Config {
	that.password = password
	return that
}

func (that *Config) SetKeepAlive(keepAlive int32) *Config {
	that.keepAlive = keepAlive
	return that
}

func (that *Config) SetCleanSession(cleanSession bool) *Config {
	that.cleanSession = cleanSession
	return that
}

func (that *Config) SetQos(qos byte) *Config {
	that.qos = qos
	return that
}

func (that *Config) SetWillTopic(willTopic string) *Config {
	that.willTopic = willTopic
	return that
}

func (that *Config) SetWillPayload(willPayload []byte) *Config {
	that.willPayload = willPayload
	return that
}

func (that *Config) SetWillQos(willQos byte) *Config {
	that.willQos = willQos
	return that
}

func (that *Config) SetWillRetain(willRetain bool) *Config {
	that.willRetain = willRetain
	return that
}

func (that *Config) SetVersion(version int) *Config {
	that.version = version
	return that
}

func (that *Config) SetTimeout(timeout int32) *Config {
	that.timeout = timeout
	return that
}

func (that *Config) SetTopics(topics ...string) *Config {
	for _, topic := range topics {
		that.AddTopic(topic)
	}
	return that
}

func (that *Config) AddTopic(topic string) {
	that.topics.PushBack(topic)
}

func (that *Config) RemoveTopic(topic string) {
	that.topics.PopAllIf(func(v string) bool {
		return v == topic
	})
}

func (that *Config) ExistsTopic(topic string) bool {
	v := that.topics.FindIf(func(v string) bool {
		return v == topic
	})
	return v != nil
}

func (that *Config) SetUseTLS(useTLS bool) *Config {
	that.useTLS = useTLS
	return that
}

func (that *Config) SetCaCertPath(caCertPath string) *Config {
	that.caCertPath = caCertPath
	return that
}

func (that *Config) SetOnAuthedCallback(onAuthed OnAuthedCallback) *Config {
	that.onAuthedCallback = onAuthed
	return that
}

func (that *Config) ClearTopics() {
	that.topics.Clear()
}

func (that *Config) Broker() string {
	return that.broker
}

func (that *Config) ClientId() string {
	return that.clientId
}

func (that *Config) Username() string {
	return that.username
}

func (that *Config) Password() string {
	return that.password
}

func (that *Config) KeepAlive() int32 {
	return that.keepAlive
}

func (that *Config) CleanSession() bool {
	return that.cleanSession
}

func (that *Config) Qos() byte {
	return that.qos
}

func (that *Config) WillTopic() string {
	return that.willTopic
}

func (that *Config) WillPayload() []byte {
	return that.willPayload
}

func (that *Config) WillQos() byte {
	return that.willQos
}

func (that *Config) WillRetain() bool {
	return that.willRetain
}

func (that *Config) Version() int {
	return that.version
}

func (that *Config) Timeout() int32 {
	return that.timeout
}

func (that *Config) Topics() []string {
	return klists.ToKSlice(that.topics)
}

func (that *Config) UseTLS() bool {
	return that.useTLS
}

func (that *Config) CaCertPath() string {
	return that.caCertPath
}

func (that *Config) OnAuthedCallback() OnAuthedCallback {
	return that.onAuthedCallback
}

func (that *Config) String() string {
	return kstrings.FormatString("Config{broker: {}, clientId: {}, username: {}, password: {}, keepAlive: {}, cleanSession: {}, qos: {}, "+
		"willTopic: {}, willPayload: {}, willQos: {}, willRetain: {}, version: {}, timeout: {}, topics:[{}], useTLS: {}, caCertPath: {}}",
		that.broker, that.clientId, that.username, that.password, that.keepAlive, that.cleanSession, that.qos,
		that.willTopic, string(that.willPayload), that.willQos, that.willRetain,
		that.version, that.timeout,
		strings.Join(klists.ToKSlice(that.topics), ", "),
		that.useTLS,
		that.caCertPath,
	)
}
