package config

import (
	"fmt"

	kconf "github.com/khan-lau/kconfig"
	mqConf "github.com/khan-lau/kmq/service/mq/config"
)

type Log struct {
	LogLevel      int8   `json:"logLevel" toml:"logLevel" yaml:"logLevel" hcl:"logLevel,attr"`                     // 日志等级
	Colorful      bool   `json:"colorful" toml:"colorful" yaml:"colorful" hcl:"colorful,attr"`                     // 是否彩色输出
	MaxAge        int    `json:"maxAge" toml:"maxAge" yaml:"maxAge" hcl:"maxAge,attr"`                             // 文件最大保存数量
	MaxSize       int64  `json:"maxSize" toml:"maxSize" yaml:"maxSize" hcl:"maxSize,attr"`                         // 文件最大滚动大小, 单位为Byte, 默认10G
	RotationTime  int    `json:"rotationTime" toml:"rotationTime" yaml:"rotationTime" hcl:"rotationTime,attr"`     // 文件最大滚动时间
	Console       bool   `json:"console" toml:"console" yaml:"console" hcl:"console,attr"`                         // 是否输出到控制台
	Async         bool   `json:"async" toml:"async" yaml:"async" hcl:"async,attr"`                                 // 是否异步输出日志
	FlushInterval int64  `json:"flushInterval" toml:"flushInterval" yaml:"flushInterval" hcl:"flushInterval,attr"` // 异步输出日志缓冲区刷新间隔, 单位为毫秒
	BufferSize    int64  `json:"bufferSize" toml:"bufferSize" yaml:"bufferSize" hcl:"bufferSize,attr"`             // 异步输出日志缓冲区大小, 单位为条数
	LogDir        string `json:"logDir" toml:"logDir" yaml:"logDir" hcl:"logDir,attr"`                             // 日志文件存储目录
}

type MQItemObj struct {
	MQType   string `json:"type" toml:"type" yaml:"type" hcl:"type,attr"`                         // 消息队列类型, 支持的类型: kafkamq, rabbitmq, redismq, rocketmq, mqtt3, natscoremq natsjsmq
	Compress bool   `json:"isCompress" toml:"isCompress" yaml:"isCompress" hcl:"isCompress,attr"` // 是否压缩
	Item     any    `json:"mq" toml:"mq" yaml:"mq" hcl:"mq,attr"`                                 // 消息队列配置
	// mqConfig any // 消息队列配置
}

func (that *MQItemObj) MQConfig() any {
	return that.Item
}

type Configure struct {
	Type           string       `json:"type" toml:"type" yaml:"type" hcl:"type,attr"`                                         // 配置类型, 支持的类型: send recv
	SyncTime       uint64       `json:"syncTime" toml:"syncTime" yaml:"syncTime" hcl:"syncTime,attr"`                         // 同步周期, 单位毫秒,不低于1000毫秒
	SyncFile       string       `json:"syncFile" toml:"syncFile" yaml:"syncFile" hcl:"syncFile,attr"`                         // 同步文件路径, 同步偏移量缓存文件路径配置
	SendInterval   uint32       `json:"sendInterval" toml:"sendInterval" yaml:"sendInterval" hcl:"sendInterval,attr"`         // 发送间隔, 单位毫秒
	SendQueueSize  uint32       `json:"sendQueueSize" toml:"sendQueueSize" yaml:"sendQueueSize" hcl:"sendQueueSize,attr"`     // 发送队列大小, 仅发送模式有效, 单位为条数, 默认值1024, 必须为2的次幂
	SendFile       string       `json:"sendFile" toml:"sendFile" yaml:"sendFile" hcl:"sendFile,attr"`                         // 发送文件路径, 仅发送模式有效
	DumpHex        bool         `json:"dumpHex" toml:"dumpHex" yaml:"dumpHex" hcl:"dumpHex,attr"`                             // 数据包是否为hexString格式; recv模式时体现在日志中; send模式时,表示 `sendFile` 中的格式是否为hexString格式
	ResetTimestamp bool         `json:"resetTimestamp" toml:"resetTimestamp" yaml:"resetTimestamp" hcl:"resetTimestamp,attr"` // 是否重置时间戳
	Log            *Log         `json:"log" toml:"log" yaml:"log" hcl:"log,attr"`                                             // 日志配置
	Source         []*MQItemObj `json:"source" toml:"source" yaml:"source" hcl:"source,block"`                                // 消息队列配置
	Target         []*MQItemObj `json:"target" toml:"target" yaml:"target" hcl:"target,block"`                                // 消息队列配置
}

func ConfigInstance(filePath string) (*Configure, error) {
	var cfg Configure
	// 调用Processor的时候, cfg已经被kconf.ReadTo反序列化填充了, 所以调用 cfg.postProcess的时候, that的成员已经填充了
	err := kconf.ReadTo(filePath, &cfg, kconf.WithProcessor(kconf.ProcessFunc(cfg.postProcess)))
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (that *Configure) postProcess(v any) error {
	cfg, ok := v.(*Configure)
	if !ok {
		return fmt.Errorf("postProcess v is not *Configure")
	}

	for _, item := range cfg.Source {
		if item == nil || item.Item == nil {
			continue
		}
		anyMap, ok := item.Item.(map[string]any)
		if !ok {
			continue
		}

		switch item.MQType {
		case "natscoremq":
			{
				var rc mqConf.NatsCoreConfig
				if err := kconf.DecodeMapToStruct(anyMap, &rc); err != nil {
					return err
				}
				item.Item = &rc
			}
		case "natsjsmq":
			{
				var rc mqConf.NatsJsConfig
				if err := kconf.DecodeMapToStruct(anyMap, &rc); err != nil {
					return err
				}
				rc.ConsumerConfig.StartWithTimestamp = -1 // 默认值设置为-1, 防御
				if rc.ConsumerConfig != nil && rc.ConsumerConfig.AutoCommit == "" {
					rc.ConsumerConfig.AutoCommit = "native"
				}
				item.Item = &rc
			}
		case "kafkamq":
			{
				var rc mqConf.KafkaConfig
				if err := kconf.DecodeMapToStruct(anyMap, &rc); err != nil {
					return err
				}
				if rc.Consumer != nil {
					if rc.Consumer.AutoCommit == "" {
						rc.Consumer.AutoCommit = "native"
					}

					if rc.Consumer.AutoCommit == "native" && rc.Consumer.AutoCommitInterval == 0 {
						rc.Consumer.AutoCommitInterval = 5000 // 默认 5000ms
					}
				}
				item.Item = &rc
			}
		case "rabbitmq":
			{
				var rc mqConf.RabbitConfig
				if err := kconf.DecodeMapToStruct(anyMap, &rc); err != nil {
					return err
				}
				if rc.Consumer != nil && rc.Consumer.AutoCommit == "" {
					rc.Consumer.AutoCommit = "native"
				}
				item.Item = &rc
			}
		case "rocketmq":
			{
				var rc mqConf.RocketConfig
				if err := kconf.DecodeMapToStruct(anyMap, &rc); err != nil {
					return err
				}
				if rc.Consumer != nil && rc.Consumer.AutoCommit == "" {
					rc.Consumer.AutoCommit = "native"
				}
				item.Item = &rc
			}
		case "redismq":
			{
				var rc mqConf.RedisConfig
				if err := kconf.DecodeMapToStruct(anyMap, &rc); err != nil {
					return err
				}
				item.Item = &rc
			}
		case "mqtt3":
			{
				var rc mqConf.MqttConfig
				if err := kconf.DecodeMapToStruct(anyMap, &rc); err != nil {
					return err
				}
				item.Item = &rc
			}
		default:
			{
				fmt.Printf("mq source type %s not support", item.MQType)
			}
		}

	}

	for _, item := range cfg.Target {
		if item == nil || item.Item == nil {
			continue
		}
		anyMap, ok := item.Item.(map[string]any)
		if !ok {
			continue
		}

		switch item.MQType {
		case "natscoremq":
			{
				var rc mqConf.NatsCoreConfig
				if err := kconf.DecodeMapToStruct(anyMap, &rc); err != nil {
					return err
				}
				item.Item = &rc
			}
		case "natsjsmq":
			{
				var rc mqConf.NatsJsConfig
				if err := kconf.DecodeMapToStruct(anyMap, &rc); err != nil {
					return err
				}
				item.Item = &rc
			}
		case "kafkamq":
			{
				var rc mqConf.KafkaConfig
				if err := kconf.DecodeMapToStruct(anyMap, &rc); err != nil {
					return err
				}
				item.Item = &rc
			}
		case "rabbitmq":
			{
				var rc mqConf.RabbitConfig
				if err := kconf.DecodeMapToStruct(anyMap, &rc); err != nil {
					return err
				}
				item.Item = &rc
			}
		case "rocketmq":
			{
				var rc mqConf.RocketConfig
				if err := kconf.DecodeMapToStruct(anyMap, &rc); err != nil {
					return err
				}
				item.Item = &rc
			}
		case "redismq":
			{
				var rc mqConf.RedisConfig
				if err := kconf.DecodeMapToStruct(anyMap, &rc); err != nil {
					return err
				}
				item.Item = &rc
			}
		case "mqtt3":
			{
				var rc mqConf.MqttConfig
				if err := kconf.DecodeMapToStruct(anyMap, &rc); err != nil {
					return err
				}
				item.Item = &rc
			}
		default:
			{
				fmt.Printf("mq target type %s not support", item.MQType)
			}
		}

	}

	if cfg.SendQueueSize < 1 {
		cfg.SendQueueSize = 1 * 1024
	}

	if cfg.Log != nil {
		if cfg.Log.FlushInterval < 1 {
			cfg.Log.FlushInterval = 1000
		}

		if cfg.Log.BufferSize < 1 {
			cfg.Log.BufferSize = 4 * 1024 * 1024
		}
	}

	return nil
}
