package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"

	toml "github.com/BurntSushi/toml"
	"github.com/khan-lau/kutils/container/kstrings"
	"github.com/khan-lau/mapstructure"
	json5 "github.com/titanous/json5"
	yaml3 "gopkg.in/yaml.v3"
)

type Log struct {
	LogLevel     int8   `json:"logLevel" toml:"logLevel" yaml:"logLevel"`             // 日志等级
	Colorful     bool   `json:"colorful" toml:"colorful" yaml:"colorful"`             // 是否彩色输出
	MaxAge       int    `json:"maxAge" toml:"maxAge" yaml:"maxAge"`                   // 文件最大保存数量
	RotationTime int    `json:"rotationTime" toml:"rotationTime" yaml:"rotationTime"` // 文件最大滚动时间
	Console      bool   `json:"console" toml:"console" yaml:"console"`                // 是否输出到控制台
	LogDir       string `json:"logDir" toml:"logDir" yaml:"logDir"`                   // 日志文件存储目录
}

type MQItemObj struct {
	MQType   string      `json:"type" toml:"type" yaml:"type"`                   // 消息队列类型, 支持的类型: kafkamq, rabbitmq, redismq, rocketmq, mqtt3, natscoremq natsjsmq
	Compress bool        `json:"isCompress" toml:"isCompress" yaml:"isCompress"` // 是否压缩
	Item     interface{} `json:"mq" toml:"mq" yaml:"mq"`                         // 消息队列配置
	// mqConfig interface{} // 消息队列配置
}

func (that *MQItemObj) MQConfig() interface{} {
	return that.Item
}

type Configure struct {
	Type           string       `json:"type" toml:"type" yaml:"type"`                               // 配置类型, 支持的类型: send recv
	SyncTime       uint64       `json:"syncTime" toml:"syncTime" yaml:"syncTime"`                   // 同步周期, 单位毫秒,不低于1000毫秒
	SyncFile       string       `json:"syncFile" toml:"syncFile" yaml:"syncFile"`                   // 同步文件路径, 同步偏移量缓存文件路径配置
	SendInterval   uint32       `json:"sendInterval" toml:"sendInterval" yaml:"sendInterval"`       // 发送间隔, 单位毫秒
	SendFile       string       `json:"sendFile" toml:"sendFile" yaml:"sendFile"`                   // 发送文件路径
	DumpHex        bool         `json:"dumpHex" toml:"dumpHex" yaml:"dumpHex"`                      // 数据包是否为hexString格式; recv模式时体现在日志中; send模式时,表示 `sendFile` 中的格式是否为hexString格式
	ResetTimestamp bool         `json:"resetTimestamp" toml:"resetTimestamp" yaml:"resetTimestamp"` // 是否重置时间戳
	Log            *Log         `json:"log" toml:"log" yaml:"log"`                                  // 日志配置
	Source         []*MQItemObj `json:"source" toml:"source" yaml:"source"`                         // 消息队列配置
	Target         []*MQItemObj `json:"target" toml:"target" yaml:"target"`                         // 消息队列配置
}

func ConfigInstance(filePath string) (*Configure, error) {
	buf, err := os.ReadFile(filePath)
	if nil != err {
		return nil, err
	}

	file_suffix := path.Ext(filePath)
	tmp := Configure{}
	if strings.ToLower(file_suffix) == ".json5" {
		err = json5.Unmarshal(buf, &tmp)
	} else if strings.ToLower(file_suffix) == ".json" {
		err = json.Unmarshal(buf, &tmp)
	} else if strings.ToLower(file_suffix) == ".yaml" {
		err = yaml3.Unmarshal(buf, &tmp)
	} else if strings.ToLower(file_suffix) == ".toml" {
		err = toml.Unmarshal(buf, &tmp)
	} else {
		return nil, fmt.Errorf("configure file format error, not [json, json5, yaml, toml]")
	}

	if nil != err {
		return nil, err
	}

	tmp.process()

	return &tmp, nil
}

func (that *Configure) process() {

	for _, item := range that.Source {
		if nil != item && nil != item.Item {
			switch item.MQType {
			case "natscoremq":
				{
					if anyMap, ok := item.Item.(map[string]interface{}); ok {
						var natscoreConf NatsCoreConfig
						_ = mapstructure.Decode(anyMap, &natscoreConf)
						item.Item = &natscoreConf
					}
				}
			case "natsjsmq":
				{
					if anyMap, ok := item.Item.(map[string]interface{}); ok {
						var natsjsConf NatsJsConfig
						_ = mapstructure.Decode(anyMap, &natsjsConf)
						natsjsConf.ConsumerConfig.StartWithTimestamp = -1 // 默认值设置为-1, 防御
						item.Item = &natsjsConf

						// decoder, _ := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
						// 	TagName: "json",
						// 	Result:  &natsjsConf,
						// 	// WeaklyTypedInput: true, // 允许宽松类型转换（例如 float64 到 int）
						// 	// WeaklyTypedInput: false, // 禁用宽松类型转换
						// 	// ErrorUnset:       true,
						// 	// ErrorUnused: true, // 报告未使用的 JSON 字段
						// 	// MatchName: func(mapKey, fieldName string) bool {
						// 	// 	return strings.EqualFold(mapKey, fieldName) // 保留大小写不敏感l
						// 	// },
						// })

						// // _ = decoder.Decode(anyMap)
						// err := decoder.Decode(anyMap)
						// if err != nil {
						// 	kstrings.Debugf("natsjs config mapstructure err: {}\n", err)
						// }
						// str := kobjs.ObjectToJson5WithoutFunc(natsjsConf)
						// kstrings.Debugf("{}\n", str)

						item.Item = &natsjsConf
					}
				}
			case "kafkamq":
				{
					if anyMap, ok := item.Item.(map[string]interface{}); ok {
						var kafkaConf KafkaConfig
						_ = mapstructure.Decode(anyMap, &kafkaConf)
						// str := kobjs.ObjectToJson5WithoutFunc(kafkaConf)
						// fmt.Println(str)
						item.Item = &kafkaConf
					}

					//if nil != item.Item.(*KafkaConfig).Net {}
				}
			case "rabbitmq":
				{
					if anyMap, ok := item.Item.(map[string]interface{}); ok {
						var rabbitConf RabbitConfig
						_ = mapstructure.Decode(anyMap, &rabbitConf)
						item.Item = &rabbitConf
					}

				}
			case "rocketmq":
				{
					if anyMap, ok := item.Item.(map[string]interface{}); ok {
						var rocketConfig RocketConfig
						_ = mapstructure.Decode(anyMap, &rocketConfig)
						item.Item = &rocketConfig
					}
				}
			case "redismq":
				{
					if anyMap, ok := item.Item.(map[string]interface{}); ok {
						var redisConf RedisConfig
						_ = mapstructure.Decode(anyMap, &redisConf)
						item.Item = &redisConf
					}

				}
			case "mqtt3":
				{
					if anyMap, ok := item.Item.(map[string]interface{}); ok {
						var mqttConf MqttConfig
						_ = mapstructure.Decode(anyMap, &mqttConf)
						item.Item = &mqttConf
					}
				}
			default:
				{
					kstrings.Debugf("mq source type {} not support", item.MQType)
				}
			}

		}
	}

	for _, item := range that.Target {
		if nil != item && nil != item.Item {
			switch item.MQType {
			case "natscoremq":
				{
					if anyMap, ok := item.Item.(map[string]interface{}); ok {
						var natscoreConf NatsCoreConfig
						_ = mapstructure.Decode(anyMap, &natscoreConf)
						item.Item = &natscoreConf
					}
				}
			case "natsjsmq":
				{
					if anyMap, ok := item.Item.(map[string]interface{}); ok {
						var natsjsConf NatsJsConfig
						_ = mapstructure.Decode(anyMap, &natsjsConf)
						item.Item = &natsjsConf
					}
				}
			case "kafkamq":
				{
					if anyMap, ok := item.Item.(map[string]interface{}); ok {
						var kafkaConf KafkaConfig
						_ = mapstructure.Decode(anyMap, &kafkaConf)
						item.Item = &kafkaConf
					}
				}
			case "rabbitmq":
				{
					if anyMap, ok := item.Item.(map[string]interface{}); ok {
						var rabbitConf RabbitConfig
						_ = mapstructure.Decode(anyMap, &rabbitConf)
						item.Item = &rabbitConf
					}
				}
			case "rocketmq":
				{
					if anyMap, ok := item.Item.(map[string]interface{}); ok {
						var rocketConfig RocketConfig
						_ = mapstructure.Decode(anyMap, &rocketConfig)
						item.Item = &rocketConfig
					}
				}
			case "redismq":
				{
					if anyMap, ok := item.Item.(map[string]interface{}); ok {
						var redisConf RedisConfig
						_ = mapstructure.Decode(anyMap, &redisConf)
						item.Item = &redisConf
					}
				}
			case "mqtt3":
				{
					if anyMap, ok := item.Item.(map[string]interface{}); ok {
						var mqttConf MqttConfig
						_ = mapstructure.Decode(anyMap, &mqttConf)
						item.Item = &mqttConf
					}
				}
			default:
				{
					kstrings.Debugf("mq target type {} not support", item.MQType)
				}
			}

		}
	}
}

///////////////////////////////////////////////////////////

type Topic struct {
	Name       string       `json:"topic" toml:"topic" yaml:"topic" mapstructure:"topic"` // 队列名称
	Partitions []*Partition `json:"partitions" toml:"partitions" yaml:"partitions"`       // 分区数量
}

type Partition struct {
	Partition int   `json:"partition" toml:"partition" yaml:"partition"` // 分区编号
	Offset    int64 `json:"offset" toml:"offset" yaml:"offset"`          // 队列偏移量
}

type Net struct {
	MaxOpenRequests int  `json:"maxOpenRequests" toml:"maxOpenRequests" yaml:"maxOpenRequests"`
	DialTimeout     int  `json:"dialTimeout" toml:"dialTimeout" yaml:"dialTimeout"`
	ReadTimeout     int  `json:"readTimeout" toml:"readTimeout" yaml:"readTimeout"`
	WriteTimeout    int  `json:"writeTimeout" toml:"writeTimeout" yaml:"writeTimeout"`
	ResolveHost     bool `json:"resolveHost" toml:"resolveHost" yaml:"resolveHost"`
}

type Producer struct {
	Topics           []*Topic `json:"topics" toml:"topics" yaml:"topics"`
	Compression      string   `json:"compression" toml:"compression" yaml:"compression"`
	CompressionLevel int      `json:"CompressionLevel" toml:"CompressionLevel" yaml:"CompressionLevel"`
	MaxMessageBytes  int      `json:"maxMessageBytes" toml:"maxMessageBytes" yaml:"maxMessageBytes"`
	RequiredAcks     string   `json:"requiredAcks" toml:"requiredAcks" yaml:"requiredAcks"`
	FlushMessages    int      `json:"flushMessages" toml:"flushMessages" yaml:"flushMessages"`
	FlushFrequency   int      `json:"flushFrequency" toml:"flushFrequency" yaml:"flushFrequency"`
	FlushMaxMessages int      `json:"flushMaxMessages" toml:"flushMaxMessages" yaml:"flushMaxMessages"`
	RetryMax         int      `json:"retryMax" toml:"retryMax" yaml:"retryMax"`
	Timeout          int      `json:"timeout" toml:"timeout" yaml:"timeout"`
}

type Consumer struct {
	Min                int      `json:"min" toml:"min" yaml:"min"`
	Max                int      `json:"max" toml:"max" yaml:"max"`
	Fetch              int      `json:"fetch" toml:"fetch" yaml:"fetch"`
	InitialOffset      int64    `json:"initialOffset" toml:"initialOffset" yaml:"initialOffset"`
	AutoCommit         bool     `json:"autoCommit" toml:"autoCommit" yaml:"autoCommit"`
	AutoCommitInterval int      `json:"autoCommitInterval" toml:"autoCommitInterval" yaml:"autoCommitInterval"`
	Assignor           string   `json:"assignor" toml:"assignor" yaml:"assignor"`
	HeartbeatInterval  int      `json:"heartbeatInterval" toml:"heartbeatInterval" yaml:"heartbeatInterval"`
	RebalanceTimeout   int      `json:"rebalanceTimeout" toml:"rebalanceTimeout" yaml:"rebalanceTimeout"`
	SessionTimeout     int      `json:"sessionTimeout" toml:"sessionTimeout" yaml:"sessionTimeout"`
	Topics             []*Topic `json:"topics" toml:"topics" yaml:"topics"`
}

type KafkaConfig struct {
	Version           string    `json:"version" toml:"version" yaml:"version"`
	ClientID          string    `json:"clientId" toml:"clientId" yaml:"clientId"`
	GroupID           string    `json:"groupId" toml:"groupId" yaml:"groupId"`
	BrokerList        []string  `json:"brokerList" toml:"brokerList" yaml:"brokerList"`
	ChannelBufferSize int       `json:"channelBufferSize" toml:"channelBufferSize" yaml:"channelBufferSize"`
	Net               *Net      `json:"net" toml:"net" yaml:"net" mapstructure:"net"`
	Consumer          *Consumer `json:"consumer" toml:"consumer" yaml:"consumer" mapstructure:"consumer"`
	Producer          *Producer `json:"producer" toml:"producer" yaml:"producer" mapstructure:"producer"`
}

///////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////

type RabbitConsumerConfig struct {
	QueueName  string `json:"queueName" toml:"queueName" yaml:"queueName"`    // 队列名称
	Exchange   string `json:"exchange" toml:"exchange" yaml:"exchange"`       // 交换机名称
	KRouterKey string `json:"kRouterKey" toml:"kRouterKey" yaml:"kRouterKey"` // 路由键
	WorkType   string `json:"workType" toml:"workType" yaml:"workType"`       // 工作模式
	AutoCommit bool   `json:"autoCommit" toml:"autoCommit" yaml:"autoCommit"` // 是否自动提交
}

type RabbitProducerConfig struct {
	Exchange string `json:"exchange" toml:"exchange" yaml:"exchange"` // 交换机名称
	Router   string `json:"router" toml:"router" yaml:"router"`       // 路由键
	WorkType string `json:"workType" toml:"workType" yaml:"workType"` // 工作模式
}

type RabbitConfig struct {
	Host     string `json:"host" toml:"host" yaml:"host"`
	Port     uint16 `json:"port" toml:"port" yaml:"port"`
	User     string `json:"user" toml:"user" yaml:"user"`
	Password string `json:"password" toml:"password" yaml:"password"`
	VHost    string `json:"vhost" toml:"vhost" yaml:"vhost"`

	Consumer *RabbitConsumerConfig `json:"consumer" toml:"consumer" yaml:"consumer" mapstructure:"consumer"` // 设置消费配置
	Producer *RabbitProducerConfig `json:"producer" toml:"producer" yaml:"producer" mapstructure:"producer"` // 设置生产配置
}

///////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////

type RedisConfig struct {
	Host     string   `json:"host" toml:"host" yaml:"host"`             // IP地址
	Port     uint16   `json:"port" toml:"port" yaml:"port"`             // 端口
	Retry    uint16   `json:"retry" toml:"retry" yaml:"retry"`          // 重连次数
	Password string   `json:"password" toml:"password" yaml:"password"` // 密码
	DbNum    uint8    `json:"dbNum" toml:"dbNum" yaml:"dbNum"`          // 数据库编号
	Topics   []string `json:"topics" toml:"topics" yaml:"topics"`       // 主题
}

///////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////

type RocketCustomConfig struct {
	Topics              []string `json:"topics" toml:"topics" yaml:"topics"`
	Mode                string   `json:"mode" toml:"mode" yaml:"mode"`                                              // BroadCasting 广播模式;  Clustering 集群模式; 默认为 Clustering
	Offset              string   `json:"offset" toml:"offset" yaml:"offset"`                                        // ConsumeFromFirstOffset 最新消息;  ConsumeFromLastOffset  最旧消息; ConsumeFromTimestamp 指定时间戳开始消费
	Timestamp           string   `json:"timestamp" toml:"timestamp" yaml:"timestamp"`                               // 指定时间戳开始消费, 格式 "20131223171201"
	Order               bool     `json:"order" toml:"order" yaml:"order"`                                           // 是否顺序消费, 默认为 false
	MessageBatchMaxSize int      `json:"messageBatchMaxSize" toml:"messageBatchMaxSize" yaml:"messageBatchMaxSize"` // 批量消费消息的最大数量, 默认为 1
	MaxReconsumeTimes   int      `json:"maxReconsumeTimes" toml:"maxReconsumeTimes" yaml:"maxReconsumeTimes"`       // 最大重消费次数, 默认为 -1
	AutoCommit          bool     `json:"autoCommit" toml:"autoCommit" yaml:"autoCommit"`                            // 是否自动提交, 默认为 false
	Interceptor         string   `json:"interceptor" toml:"interceptor" yaml:"interceptor"`                         // 消息拦截器, 默认为空
}

type RocketProducerConfig struct {
	Topics        []string `json:"topics" toml:"topics" yaml:"topics"`
	Timeout       int      `json:"timeout" toml:"timeout" yaml:"timeout"`                   // 消息发送超时时间
	Retry         int      `json:"retry" toml:"retry" yaml:"retry"`                         // 消息发送重试次数
	QueueSelector string   `json:"queueSelector" toml:"queueSelector" yaml:"queueSelector"` // 消息队列选择策略, RandomQueueSelector 随机选择队列; RoundRobinQueueSelector 按照轮训方式选择队列; ManualQueueSelector 直接选择消息中配置的队列
	Interceptor   string   `json:"interceptor" toml:"interceptor" yaml:"interceptor"`       // 消息拦截器, 默认为空
	AsyncSend     bool     `json:"asyncSend" toml:"asyncSend" yaml:"asyncSend"`             // 是否异步发送消息, 默认为 false
}

type RocketConfig struct {
	NsResolver bool     `json:"nsResolver" toml:"nsResolver" yaml:"nsResolver"`
	Servers    []string `json:"servers" toml:"servers" yaml:"servers"`
	GroupName  string   `json:"groupName" toml:"groupName" yaml:"groupName"`
	ClientID   string   `json:"clientID" toml:"clientID" yaml:"clientID"`
	Namespace  string   `json:"namespace" toml:"namespace" yaml:"namespace"` // 命名空间

	AccessKey string `json:"accessKey" toml:"accessKey" yaml:"accessKey"`
	SecretKey string `json:"secretKey" toml:"secretKey" yaml:"secretKey"`

	Consumer *RocketCustomConfig   `json:"consumer" toml:"consumer" yaml:"consumer" mapstructure:"consumer"`
	Producer *RocketProducerConfig `json:"producer" toml:"producer" yaml:"producer" mapstructure:"producer"`
}

///////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////

type MqttConfig struct {
	Broker       string `json:"broker" toml:"broker" yaml:"broker"`                   // Broker 地址，例如 "127.0.0.1:1883"
	ClientID     string `json:"clientId" toml:"clientId" yaml:"clientId"`             // 客户端ID，用于唯一标识一个MQTT连接
	UserName     string `json:"userName" toml:"userName" yaml:"userName"`             // 用户名，用于连接MQTT服务器时进行身份验证
	Password     string `json:"password" toml:"password" yaml:"password"`             // 密码，用于连接MQTT服务器时进行身份验证
	KeepAlive    int    `json:"keepAlive" toml:"keepAlive" yaml:"keepAlive"`          // 心跳间隔，单位为毫秒。客户端和服务器之间保持连接的心跳时间
	CleanSession bool   `json:"cleanSession" toml:"cleanSession" yaml:"cleanSession"` // 是否清除会话，如果为true，则断开连接后之前的订阅和消息都会被清空
	Qos          byte   `json:"qos" toml:"qos" yaml:"qos"`                            // 消息服务质量等级，0表示最多一次，1表示至少一次，2表示恰好一次
	Version      int    `json:"version" toml:"version" yaml:"version"`                // 协议版本 3: 3.1; 4: 3.1.1; 5: 5.0

	WillTopic   string `json:"willTopic" toml:"willTopic" yaml:"willTopic"`       // 遗嘱消息的主题，当客户端意外断开连接时，服务器会发布此主题的消息
	WillPayload string `json:"willPayload" toml:"willPayload" yaml:"willPayload"` // 遗嘱消息的内容，当客户端意外断开连接时，服务器会发布此内容的消息
	WillQos     byte   `json:"willQos" toml:"willQos" yaml:"willQos"`             // 遗嘱消息的服务质量等级，0表示最多一次，1表示至少一次，2表示恰好一次
	WillRetain  bool   `json:"willRetain" toml:"willRetain" yaml:"willRetain"`    // 遗嘱消息是否保留，如果为true，则服务器会将此消息保存到持久存储中

	Timeout    int32    `json:"timeout" toml:"timeout" yaml:"timeout"`          // 通信超时时间，单位为毫秒
	Topics     []string `json:"topics" toml:"topics" yaml:"topics"`             // 订阅的主题列表
	UseTLS     bool     `json:"useTLS" toml:"useTLS" yaml:"useTLS"`             // 是否启用 TLS
	CaCertPath string   `json:"caCertPath" toml:"caCertPath" yaml:"caCertPath"` // CA 证书路径, 仅当 useTLS 为 true 时有效
}

///////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////

type NatsCoreConfig struct {
	ClientID           string   `json:"clientId" toml:"clientId" yaml:"clientId"`                               // 客户端ID，用于唯一标识一个NATS连接
	User               string   `json:"user" toml:"user" yaml:"user"`                                           // 用户名可以为空
	Password           string   `json:"password" toml:"password" yaml:"password"`                               // 密码或token
	BrokerList         []string `json:"brokerList" toml:"brokerList" yaml:"brokerList"`                         // 服务器地址列表，支持多个地址, 如 "nats://127.0.0.1:4222,nats://127.0.0.1:4223,tls://127.0.0.1
	AllowReconnect     bool     `json:"allowReconnect" toml:"allowReconnect" yaml:"allowReconnect"`             // 默认不启用自动重连
	MaxReconnect       int      `json:"maxReconnect" toml:"maxReconnect" yaml:"maxReconnect"`                   // 最大重连次数，0 表示不启用自动重连
	ReconnectWait      int64    `json:"reconnectWait" toml:"reconnectWait" yaml:"reconnectWait"`                // 重连等待时间，单位毫秒
	ReconnectBufSize   int      `json:"reconnectBufSize" toml:"reconnectBufSize" yaml:"reconnectBufSize"`       // 在客户端与服务器连接断开时，临时缓存你发布的出站（outgoing）消息, -1 不启用自动重连缓冲区, 默认8M缓冲区
	ConnectTimeout     int64    `json:"connectTimeout" toml:"connectTimeout" yaml:"connectTimeout"`             // 连接超时时间，单位为毫秒
	PingInterval       int64    `json:"pingInterval" toml:"pingInterval" yaml:"pingInterval"`                   // ping间隔时间, 单位毫秒, 默认为2分钟
	MaxPingsOut        int      `json:"maxPingsOut" toml:"maxPingsOut" yaml:"maxPingsOut"`                      // 最大允许的ping无应答次数, 超过则断开连接
	UseTls             bool     `json:"useTls" toml:"useTls" yaml:"useTls"`                                     // 默认不启用 TLS 加密连接
	CaCertPath         string   `json:"caCertPath" toml:"caCertPath" yaml:"caCertPath"`                         // CA 证书路径, 仅当 useTLS 为 true 时有效
	TlsClientCert      string   `json:"tlsClientCert" toml:"tlsClientCert" yaml:"tlsClientCert"`                // 客户端证书路径, 仅当 useTLS 为 true 时有效
	KeyPath            string   `json:"keyPath" toml:"keyPath" yaml:"keyPath"`                                  // 密钥表路径, 仅当 useTLS 为 true 时有效
	InsecureSkipVerify bool     `json:"insecureSkipVerify" toml:"insecureSkipVerify" yaml:"insecureSkipVerify"` // 忽略证书验证，默认为 false
	MinTlsVer          int      `json:"minTlsVer" toml:"minTlsVer" yaml:"minTlsVer"`                            // 最小TLS版本，默认为1.2 VersionTLS10 = 0x0301, VersionTLS11 = 0x0302, VersionTLS12 = 0x0303, VersionTLS13 = 0x0304

	Topics     []string `json:"topics" toml:"topics" yaml:"topics"`             // 主题列表
	QueueGroup string   `json:"queueGroup" toml:"queueGroup" yaml:"queueGroup"` // 消费组名称, 用于负载均衡, 允许空字符串, 为空时为广播模式, 非空时为负载均衡模式, core模式下 没有 类似kafka的通过key的hash实现的负载均衡模式
	MaxPending int      `json:"maxPending" toml:"maxPending" yaml:"maxPending"` // 最大等待的消息数量，默认为 0 (无限)
}

///////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////

type NatsJsConsumerConfig struct {
	GroupId            string `json:"groupId" toml:"groupId" yaml:"groupId"`          // 持久化订阅名称，可以为空, 等同于消费者名称, 为空时表示临时消费组, 最后一个消费者断开连接时, 未处理消息被丢弃
	MaxWait            int    `json:"maxWait" toml:"maxWait" yaml:"maxWait"`          // 最大等待时间，默认为 -1 (无限)
	StartWithTimestamp int64  `mapstructure:"-" json:"-"`                             // 最后消费的时间戳, 精度为纳秒, 默认为 -1 (无效)
	AutoCommit         bool   `json:"autoCommit" toml:"autoCommit" yaml:"autoCommit"` // 是否自动提交消息，默认为 false
	AckPolicy          string `json:"ackPolicy" toml:"ackPolicy" yaml:"ackPolicy"`    // 确认策略，默认`none`: 自动ack; `all`: 确认一个序列号时，会隐式确认该序列号之前的所有消息; `explicit`: 每条需要单独确认
	DeliverPolicy      string `mapstructure:"-" json:"-"`                             // `json:"deliverPolicy" toml:"deliverPolicy" yaml:"deliverPolicy"` // 投递策略，固定为 `by_start_time`
	// `all`, 从第一条开始消费;  与 new不同
	// `last` 从最后一条开始消费;
	// `new` 最新一条开始, 从创建消费开始的第一条;
	// `by_start_sequence` 从指定序列号消费;
	// `by_start_time`; 从指定时间戳消费;
	// `last_per_subject`: 从每个主题的最后一条开始消费
}

type NatsJsConfig struct {
	ClientID           string   `json:"clientId" toml:"clientId" yaml:"clientId"`                               // 客户端ID，用于唯一标识一个NATS连接
	User               string   `json:"user" toml:"user" yaml:"user"`                                           // 用户名可以为空
	Password           string   `json:"password" toml:"password" yaml:"password"`                               // 密码或token
	BrokerList         []string `json:"brokerList" toml:"brokerList" yaml:"brokerList"`                         // 服务器地址列表，支持多个地址, 如 "nats://127.0.0.1:4222,nats://127.0.0.1:4223,tls://127.0.0.1
	AllowReconnect     bool     `json:"allowReconnect" toml:"allowReconnect" yaml:"allowReconnect"`             // 默认不启用自动重连
	MaxReconnect       int      `json:"maxReconnect" toml:"maxReconnect" yaml:"maxReconnect"`                   // 最大重连次数，0 表示不启用自动重连
	ReconnectWait      int64    `json:"reconnectWait" toml:"reconnectWait" yaml:"reconnectWait"`                // 重连等待时间，单位毫秒
	ReconnectBufSize   int      `json:"reconnectBufSize" toml:"reconnectBufSize" yaml:"reconnectBufSize"`       // 在客户端与服务器连接断开时，临时缓存你发布的出站（outgoing）消息, -1 不启用自动重连缓冲区, 默认8M缓冲区
	ConnectTimeout     int64    `json:"connectTimeout" toml:"connectTimeout" yaml:"connectTimeout"`             // 连接超时时间，单位为毫秒
	PingInterval       int64    `json:"pingInterval" toml:"pingInterval" yaml:"pingInterval"`                   // ping间隔时间, 单位毫秒, 默认为2分钟
	MaxPingsOut        int      `json:"maxPingsOut" toml:"maxPingsOut" yaml:"maxPingsOut"`                      // 最大允许的ping无应答次数, 超过则断开连接
	UseTls             bool     `json:"useTls" toml:"useTls" yaml:"useTls"`                                     // 默认不启用 TLS 加密连接
	CaCertPath         string   `json:"caCertPath" toml:"caCertPath" yaml:"caCertPath"`                         // CA 证书路径, 仅当 useTLS 为 true 时有效
	TlsClientCert      string   `json:"tlsClientCert" toml:"tlsClientCert" yaml:"tlsClientCert"`                // 客户端证书路径, 仅当 useTLS 为 true 时有效
	KeyPath            string   `json:"keyPath" toml:"keyPath" yaml:"keyPath"`                                  // 密钥表路径, 仅当 useTLS 为 true 时有效
	InsecureSkipVerify bool     `json:"insecureSkipVerify" toml:"insecureSkipVerify" yaml:"insecureSkipVerify"` // 忽略证书验证，默认为 false
	MinTlsVer          int      `json:"minTlsVer" toml:"minTlsVer" yaml:"minTlsVer"`                            // 最小TLS版本，默认为1.2 VersionTLS10 = 0x0301, VersionTLS11 = 0x0302, VersionTLS12 = 0x0303, VersionTLS13 = 0x0304

	QueueName          string                `json:"queueName" toml:"queueName" yaml:"queueName"`                            // 队列名称, jetstream名称, 必须不可为空, 命名不可包含 ` `, `.`, `*`, `>`, `\`, `/` 及非可见字符
	StorageType        string                `json:"storageType" toml:"storageType" yaml:"storageType"`                      // 存储类型，默认为 "memory", 可选 "file"
	StorageCompression string                `json:"storageCompression" toml:"storageCompression" yaml:"storageCompression"` // 存储压缩，默认为 "none", 可选 "s2"
	RetentionPolicy    string                `json:"retentionPolicy" toml:"retentionPolicy" yaml:"retentionPolicy"`          // 保留策略，默认为 "limits", 可选 "interest" 或 "workqueue"
	MaxConsumers       int                   `json:"maxConsumers" toml:"maxConsumers" yaml:"maxConsumers"`                   // 最大消费者数，默认为 -1 (无限)
	MaxMsgs            int64                 `json:"maxMsgs" toml:"maxMsgs" yaml:"maxMsgs"`                                  // 最大消息数，默认为 -1 (无限)
	MaxBytes           int64                 `json:"maxBytes" toml:"maxBytes" yaml:"maxBytes"`                               // 最大字节数，默认为 -1 (无限)
	MaxAge             int64                 `json:"maxAge" toml:"maxAge" yaml:"maxAge"`                                     // 最大消息生命周期，默认为 -1 (无限)
	MaxMsgsPerSubject  int64                 `json:"maxMsgsPerSubject" toml:"maxMsgsPerSubject" yaml:"maxMsgsPerSubject"`    // 每个主题的最大消息数，默认为 -1 (无限)
	MaxMsgSize         int32                 `json:"maxMsgSize" toml:"maxMsgSize" yaml:"maxMsgSize"`                         // 最大消息大小，默认为 -1 (无限)
	Duplicates         int64                 `json:"duplicates" toml:"duplicates" yaml:"duplicates"`                         // 多长时间内不允许消息重复, 单位MS, 默认为 -1 (无限)
	Discard            string                `json:"discard" toml:"discard" yaml:"discard"`                                  // 丢弃策略，默认为 "old", 可选 "new"
	Topics             []string              `json:"topics" toml:"topics" yaml:"topics"`                                     // 主题列表，默认为空, nats支持一个stream对多个consmer, 但此处只实现了一个stream对应一个consumer
	ConsumerConfig     *NatsJsConsumerConfig `json:"consumer" toml:"consumer" yaml:"consumer" mapstructure:"consumer"`       // 消费者配置
}
