package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"

	toml "github.com/BurntSushi/toml"
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

type RDB struct {
	Url         string `json:"url" toml:"url" yaml:"url"`                         // 数据库连接
	MaxConn     int    `json:"maxConn" toml:"maxConn" yaml:"maxConn"`             // 最大连接数
	MaxIdle     int    `json:"maxIdle" toml:"maxIdle" yaml:"maxIdle"`             // 最大空闲连接数
	MaxIdleTime int    `json:"maxIdleTime" toml:"maxIdleTime" yaml:"maxIdleTime"` // 最大空闲时间, 单位毫秒
	MaxLifetime int    `json:"maxLifetime" toml:"maxLifetime" yaml:"maxLifetime"` // 最长生命周期, 单位毫秒
	Schema      string `json:"schema" toml:"schema" yaml:"schema"`                // 数据库schema
}

type WebRPC struct {
	IP   string `json:"ip" toml:"ip" yaml:"ip"`       // ip地址
	Port uint16 `json:"port" toml:"port" yaml:"port"` // 端口号
}

type MQItemObj struct {
	MQType string      `json:"type" toml:"type" yaml:"type"` // 消息队列类型, 支持的类型: kafkamq, rabbitmq, redismq, rocketmq, mqtt3
	Item   interface{} `json:"mq" toml:"mq" yaml:"mq"`       // 消息队列配置
	// mqConfig interface{} // 消息队列配置
}

func (that *MQItemObj) MQConfig() interface{} {
	return that.Item
}

type Configure struct {
	Log       *Log         `json:"log" toml:"log" yaml:"log"`                   // 日志配置
	RDB       *RDB         `json:"rdb" toml:"rdb" yaml:"rdb"`                   // 数据库配置
	QueueSize int          `json:"queueSize" toml:"queueSize" yaml:"queueSize"` // 消息队列大小, 不超过cpu核心数的2倍
	RPC       *WebRPC      `json:"rpc" toml:"rpc" yaml:"rpc"`                   // rpc配置
	Source    []*MQItemObj `json:"source" toml:"source" yaml:"source"`          // 消息队列配置
	Target    []*MQItemObj `json:"target" toml:"target" yaml:"target"`          // 消息队列配置
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
			}

		}
	}

	for _, item := range that.Target {
		if nil != item && nil != item.Item {
			switch item.MQType {
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
			}
		}
	}
}

///////////////////////////////////////////////////////////

type Topic struct {
	Name      string `json:"topic" toml:"topic" yaml:"topic"`             // 队列名称
	Partition int    `json:"partition" toml:"partition" yaml:"partition"` // 分区数量
	Offset    int64  `json:"offset" toml:"offset" yaml:"offset"`          // 队列偏移量
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
	Net               *Net      `json:"net" toml:"net" yaml:"net"`
	Consumer          *Consumer `json:"consumer" toml:"consumer" yaml:"consumer"`
	Producer          *Producer `json:"producer" toml:"producer" yaml:"producer"`
}

///////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////

type RabbitConsumerConfig struct {
	QueueName  string `json:"queueName" toml:"queueName" yaml:"queueName"`    // 队列名称
	Exchange   string `json:"exchange" toml:"exchange" yaml:"exchange"`       // 交换机名称
	KRouterKey string `json:"kRouterKey" toml:"kRouterKey" yaml:"kRouterKey"` // 路由键
	WorkType   string `json:"workType" toml:"workType" yaml:"workType"`       // 工作模式
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

	Consumer *RabbitConsumerConfig `json:"consumer" toml:"consumer" yaml:"consumer"` // 设置消费配置
	Producer *RabbitProducerConfig `json:"producer" toml:"producer" yaml:"producer"` // 设置生产配置
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

	Consumer *RocketCustomConfig   `json:"consumer" toml:"consumer" yaml:"consumer"`
	Producer *RocketProducerConfig `json:"producer" toml:"producer" yaml:"producer"`
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
