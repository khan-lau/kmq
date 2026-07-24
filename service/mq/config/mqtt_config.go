package config

type MqttConfig struct {
	Broker       string `json:"broker" toml:"broker" yaml:"broker" hcl:"broker,attr"`             // Broker 地址，例如 "127.0.0.1:1883"
	ClientID     string `json:"clientId" toml:"clientId" yaml:"clientId" hcl:"clientId,attr"`     // 客户端ID，用于唯一标识一个MQTT连接
	UserName     string `json:"userName" toml:"userName" yaml:"userName" hcl:"userName,attr"`     // 用户名，用于连接MQTT服务器时进行身份验证
	Password     string `json:"password" toml:"password" yaml:"password" hcl:"password,attr"`     // 密码，用于连接MQTT服务器时进行身份验证
	KeepAlive    int    `json:"keepAlive" toml:"keepAlive" yaml:"keepAlive" hcl:"keepAlive,attr"` // 心跳间隔，单位为毫秒。客户端和服务器之间保持连接的心跳时间
	CleanSession bool   `json:"cleanSession" toml:"cleanSession" yaml:"cleanSession"`             // 是否清除会话，如果为true，则断开连接后之前的订阅和消息都会被清空
	Qos          byte   `json:"qos" toml:"qos" yaml:"qos" hcl:"qos,attr"`                         // 消息服务质量等级，0表示最多一次，1表示至少一次，2表示恰好一次
	Version      int    `json:"version" toml:"version" yaml:"version" hcl:"version,attr"`         // 协议版本 3: 3.1; 4: 3.1.1; 5: 5.0

	WillTopic   string `json:"willTopic" toml:"willTopic" yaml:"willTopic" hcl:"willTopic,attr"`         // 遗嘱消息的主题，当客户端意外断开连接时，服务器会发布此主题的消息
	WillPayload string `json:"willPayload" toml:"willPayload" yaml:"willPayload" hcl:"willPayload,attr"` // 遗嘱消息的内容，当客户端意外断开连接时，服务器会发布此内容的消息
	WillQos     byte   `json:"willQos" toml:"willQos" yaml:"willQos" hcl:"willQos,attr"`                 // 遗嘱消息的服务质量等级，0表示最多一次，1表示至少一次，2表示恰好一次
	WillRetain  bool   `json:"willRetain" toml:"willRetain" yaml:"willRetain" hcl:"willRetain,attr"`     // 遗嘱消息是否保留，如果为true，则服务器会将此消息保存到持久存储中

	Timeout    int32    `json:"timeout" toml:"timeout" yaml:"timeout" hcl:"timeout,attr"`             // 通信超时时间，单位为毫秒
	Topics     []string `json:"topics" toml:"topics" yaml:"topics" hcl:"topics,attr"`                 // 订阅的主题列表
	UseTLS     bool     `json:"useTLS" toml:"useTLS" yaml:"useTLS" hcl:"useTLS,attr"`                 // 是否启用 TLS
	CaCertPath string   `json:"caCertPath" toml:"caCertPath" yaml:"caCertPath" hcl:"caCertPath,attr"` // CA 证书路径, 仅当 useTLS 为 true 时有效
}
