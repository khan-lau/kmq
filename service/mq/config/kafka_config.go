package config

import (
	"fmt"
	"strings"
)

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
	CompressionLevel int      `json:"compressionLevel" toml:"compressionLevel" yaml:"compressionLevel"`
	MaxMessageBytes  int      `json:"maxMessageBytes" toml:"maxMessageBytes" yaml:"maxMessageBytes"`
	RequiredAcks     string   `json:"requiredAcks" toml:"requiredAcks" yaml:"requiredAcks"`
	Idempotent       bool     `json:"idempotent" toml:"idempotent" yaml:"idempotent"`    // 是否开启幂等性, 默认为false, 开启后, 消息会在 Net.MaxOpenRequests大于1时, 按顺序发送, 但性能会有小幅下降
	ReturnAck        bool     `json:"returnAck" toml:"returnAck" yaml:"returnAck"`       // 是否返回ack信息
	ReturnError      bool     `json:"returnError" toml:"returnError" yaml:"returnError"` // 是否返回错误信息
	FlushMessages    int      `json:"flushMessages" toml:"flushMessages" yaml:"flushMessages"`
	FlushFrequency   int      `json:"flushFrequency" toml:"flushFrequency" yaml:"flushFrequency"`
	FlushMaxMessages int      `json:"flushMaxMessages" toml:"flushMaxMessages" yaml:"flushMaxMessages"`
	RetryMax         int      `json:"retryMax" toml:"retryMax" yaml:"retryMax"`
	Timeout          int      `json:"timeout" toml:"timeout" yaml:"timeout"`
}

type Consumer struct {
	MaxProcessingTime  int      `json:"maxProcessingTime" toml:"maxProcessingTime" yaml:"maxProcessingTime"`    // 消费者处理消息的最大时间, 超过后会触发再均衡, 单位ms
	Min                int      `json:"min" toml:"min" yaml:"min"`                                              // 每次从broker拉取的最小字节数
	Max                int      `json:"max" toml:"max" yaml:"max"`                                              // 每次从broker拉取的最大字节数
	Fetch              int      `json:"fetch" toml:"fetch" yaml:"fetch"`                                        // 每次从broker拉取的字节数
	MaxWaitTime        int      `json:"maxWaitTime" toml:"maxWaitTime" yaml:"maxWaitTime"`                      // 拉取消息时最大等待时间, 单位ms, 默认250ms
	InitialOffset      int64    `json:"initialOffset" toml:"initialOffset" yaml:"initialOffset"`                // 初始偏移量, 选项: -1: 从最新消息开始消费, -2: 从最早消息开始消费, other: 指定偏移量
	AutoCommit         string   `json:"autoCommit" toml:"autoCommit" yaml:"autoCommit"`                         // 自动commit, 支持 native:原生自动提交, custom: 客户端实现自动提交, none: 手动提交
	AutoCommitInterval int      `json:"autoCommitInterval" toml:"autoCommitInterval" yaml:"autoCommitInterval"` // 原生自动提交的 提交间隔, 单位ms
	ReturnError        bool     `json:"returnError" toml:"returnError" yaml:"returnError"`                      // 是否返回错误信息
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
