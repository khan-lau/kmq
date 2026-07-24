package config

import (
	"fmt"
	"strings"
)

type Topic struct {
	Name       string       `json:"topic" toml:"topic" yaml:"topic" mapstructure:"topic" hcl:"topic,attr"` // 队列名称
	Partitions []*Partition `json:"partitions" toml:"partitions" yaml:"partitions" hcl:"partitions,block"` // 分区数量
}

type Partition struct {
	Partition int   `json:"partition" toml:"partition" yaml:"partition" hcl:"partition,attr"` // 分区编号
	Offset    int64 `json:"offset" toml:"offset" yaml:"offset" hcl:"offset,attr"`             // 队列偏移量
}

type Net struct {
	MaxOpenRequests int  `json:"maxOpenRequests" toml:"maxOpenRequests" yaml:"maxOpenRequests" hcl:"maxOpenRequests,attr"` // 最大并发请求数
	DialTimeout     int  `json:"dialTimeout" toml:"dialTimeout" yaml:"dialTimeout" hcl:"dialTimeout,attr"`                 // 连接超时时间, 单位ms
	ReadTimeout     int  `json:"readTimeout" toml:"readTimeout" yaml:"readTimeout" hcl:"readTimeout,attr"`                 // 读超超时时间, 单位ms
	WriteTimeout    int  `json:"writeTimeout" toml:"writeTimeout" yaml:"writeTimeout" hcl:"writeTimeout,attr"`             // 写超超时时间, 单位ms
	KeepAlive       int  `json:"keepAlive" toml:"keepAlive" yaml:"keepAlive" hcl:"keepAlive,attr"`                         // 保持连接时间间隔, 单位ms
	ResolveHost     bool `json:"resolveHost" toml:"resolveHost" yaml:"resolveHost" hcl:"resolveHost,attr"`                 // 是否解析主机
}

type Producer struct {
	Topics           []*Topic `json:"topics" toml:"topics" yaml:"topics" hcl:"topics,block"`
	Compression      string   `json:"compression" toml:"compression" yaml:"compression" hcl:"compression,attr"`
	CompressionLevel int      `json:"compressionLevel" toml:"compressionLevel" yaml:"compressionLevel" hcl:"compressionLevel,attr"`
	MaxMessageBytes  int      `json:"maxMessageBytes" toml:"maxMessageBytes" yaml:"maxMessageBytes" hcl:"maxMessageBytes,attr"`
	RequiredAcks     string   `json:"requiredAcks" toml:"requiredAcks" yaml:"requiredAcks" hcl:"requiredAcks,attr"`
	Idempotent       bool     `json:"idempotent" toml:"idempotent" yaml:"idempotent" hcl:"idempotent,attr"`     // 是否开启幂等性, 默认为false, 开启后, 消息会在 Net.MaxOpenRequests大于1时, 按顺序发送, 但性能会有小幅下降
	ReturnAck        bool     `json:"returnAck" toml:"returnAck" yaml:"returnAck" hcl:"returnAck,attr"`         // 是否返回ack信息
	ReturnError      bool     `json:"returnError" toml:"returnError" yaml:"returnError" hcl:"returnError,attr"` // 是否返回错误信息
	FlushMessages    int      `json:"flushMessages" toml:"flushMessages" yaml:"flushMessages" hcl:"flushMessages,attr"`
	FlushFrequency   int      `json:"flushFrequency" toml:"flushFrequency" yaml:"flushFrequency" hcl:"flushFrequency,attr"`
	FlushMaxMessages int      `json:"flushMaxMessages" toml:"flushMaxMessages" yaml:"flushMaxMessages" hcl:"flushMaxMessages,attr"`
	RetryMax         int      `json:"retryMax" toml:"retryMax" yaml:"retryMax" hcl:"retryMax,attr"`
	Timeout          int      `json:"timeout" toml:"timeout" yaml:"timeout" hcl:"timeout,attr"`
}

type Consumer struct {
	MaxProcessingTime  int      `json:"maxProcessingTime" toml:"maxProcessingTime" yaml:"maxProcessingTime" hcl:"maxProcessingTime,attr"`     // 消费者处理消息的最大时间, 超过后会触发再均衡, 单位ms
	Min                int      `json:"min" toml:"min" yaml:"min" hcl:"min,attr"`                                                             // 每次从broker拉取的最小字节数
	Max                int      `json:"max" toml:"max" yaml:"max" hcl:"max,attr"`                                                             // 每次从broker拉取的最大字节数
	Fetch              int      `json:"fetch" toml:"fetch" yaml:"fetch" hcl:"fetch,attr"`                                                     // 每次从broker拉取的字节数
	MaxWaitTime        int      `json:"maxWaitTime" toml:"maxWaitTime" yaml:"maxWaitTime" hcl:"maxWaitTime,attr"`                             // 拉取消息时最大等待时间, 单位ms, 默认250ms
	InitialOffset      int64    `json:"initialOffset" toml:"initialOffset" yaml:"initialOffset" hcl:"initialOffset,attr"`                     // 初始偏移量, 选项: -1: 从最新消息开始消费, -2: 从最早消息开始消费, other: 指定偏移量
	AutoCommit         string   `json:"autoCommit" toml:"autoCommit" yaml:"autoCommit" hcl:"autoCommit,attr"`                                 // 自动commit, 支持 native:原生自动提交, custom: 客户端实现自动提交, none: 手动提交
	AutoCommitInterval int      `json:"autoCommitInterval" toml:"autoCommitInterval" yaml:"autoCommitInterval" hcl:"autoCommitInterval,attr"` // 原生自动提交的 提交间隔, 单位ms
	ReturnError        bool     `json:"returnError" toml:"returnError" yaml:"returnError" hcl:"returnError,attr"`                             // 是否返回错误信息
	Assignor           string   `json:"assignor" toml:"assignor" yaml:"assignor" hcl:"assignor,attr"`                                         // 分区分配器, 选项: roundrobin: 轮询分配, sticky: 粘性分配, none: 手动分配
	HeartbeatInterval  int      `json:"heartbeatInterval" toml:"heartbeatInterval" yaml:"heartbeatInterval" hcl:"heartbeatInterval,attr"`     // 心跳间隔, 单位ms
	RebalanceTimeout   int      `json:"rebalanceTimeout" toml:"rebalanceTimeout" yaml:"rebalanceTimeout" hcl:"rebalanceTimeout,attr"`         // 再均衡超时时间, 单位ms
	SessionTimeout     int      `json:"sessionTimeout" toml:"sessionTimeout" yaml:"sessionTimeout" hcl:"sessionTimeout,attr"`                 // 会话超时时间, 单位ms
	Topics             []*Topic `json:"topics" toml:"topics" yaml:"topics" hcl:"topics,block"`
}

type KafkaConfig struct {
	Version           string    `json:"version" toml:"version" yaml:"version" hcl:"version,attr"`
	ClientID          string    `json:"clientId" toml:"clientId" yaml:"clientId" hcl:"clientId,attr"`
	GroupID           string    `json:"groupId" toml:"groupId" yaml:"groupId" hcl:"groupId,attr"`
	BrokerList        []string  `json:"brokerList" toml:"brokerList" yaml:"brokerList" hcl:"brokerList,attr"`
	ChannelBufferSize int       `json:"channelBufferSize" toml:"channelBufferSize" yaml:"channelBufferSize" hcl:"channelBufferSize,attr"`
	Net               *Net      `json:"net" toml:"net" yaml:"net" mapstructure:"net" hcl:"net,block"`
	Consumer          *Consumer `json:"consumer" toml:"consumer" yaml:"consumer" mapstructure:"consumer" hcl:"consumer,block"`
	Producer          *Producer `json:"producer" toml:"producer" yaml:"producer" mapstructure:"producer" hcl:"producer,block"`
}

func KafkaTopicsToStr(topics []*Topic) string {
	var sb strings.Builder
	_ = sb.WriteByte('[')
	for i, topic := range topics {
		if i > 0 {
			sb.WriteString(", ")
		}
		_, _ = sb.WriteString("topic:")
		_, _ = sb.WriteString(topic.Name)
		_, _ = sb.WriteString(", partitions:[")
		for j, partition := range topic.Partitions {
			if j > 0 {
				_, _ = sb.WriteString(", ")
			}
			_, _ = fmt.Fprintf(&sb, "%d:%d", partition.Partition, partition.Offset)
		}
		_ = sb.WriteByte(']')
	}
	_ = sb.WriteByte(']')
	return sb.String()
}
