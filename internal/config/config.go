package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"

	toml "github.com/BurntSushi/toml"
	conf "github.com/khan-lau/kmq/service/mq/config"
	"github.com/khan-lau/mapstructure"
	json5 "github.com/titanous/json5"
	yaml3 "gopkg.in/yaml.v3"
)

type Log struct {
	LogLevel      int8   `json:"logLevel" toml:"logLevel" yaml:"logLevel"`                // 日志等级
	Colorful      bool   `json:"colorful" toml:"colorful" yaml:"colorful"`                // 是否彩色输出
	MaxAge        int    `json:"maxAge" toml:"maxAge" yaml:"maxAge"`                      // 文件最大保存数量
	MaxSize       int64  `json:"maxSize" toml:"maxSize" yaml:"maxSize"`                   // 文件最大滚动大小, 单位为Byte, 默认10G
	RotationTime  int    `json:"rotationTime" toml:"rotationTime" yaml:"rotationTime"`    // 文件最大滚动时间
	Console       bool   `json:"console" toml:"console" yaml:"console"`                   // 是否输出到控制台
	Async         bool   `json:"async" toml:"async" yaml:"async"`                         // 是否异步输出日志
	FlushInterval int64  `json:"flushInterval" toml:"flushInterval" yaml:"flushInterval"` // 异步输出日志缓冲区刷新间隔, 单位为毫秒
	BufferSize    int64  `json:"bufferSize" toml:"bufferSize" yaml:"bufferSize"`          // 异步输出日志缓冲区大小, 单位为条数
	LogDir        string `json:"logDir" toml:"logDir" yaml:"logDir"`                      // 日志文件存储目录
}

type MQItemObj struct {
	MQType   string `json:"type" toml:"type" yaml:"type"`                   // 消息队列类型, 支持的类型: kafkamq, rabbitmq, redismq, rocketmq, mqtt3, natscoremq natsjsmq
	Compress bool   `json:"isCompress" toml:"isCompress" yaml:"isCompress"` // 是否压缩
	Item     any    `json:"mq" toml:"mq" yaml:"mq"`                         // 消息队列配置
	// mqConfig any // 消息队列配置
}

func (that *MQItemObj) MQConfig() any {
	return that.Item
}

type Configure struct {
	Type           string       `json:"type" toml:"type" yaml:"type"`                               // 配置类型, 支持的类型: send recv
	SyncTime       uint64       `json:"syncTime" toml:"syncTime" yaml:"syncTime"`                   // 同步周期, 单位毫秒,不低于1000毫秒
	SyncFile       string       `json:"syncFile" toml:"syncFile" yaml:"syncFile"`                   // 同步文件路径, 同步偏移量缓存文件路径配置
	SendInterval   uint32       `json:"sendInterval" toml:"sendInterval" yaml:"sendInterval"`       // 发送间隔, 单位毫秒
	SendQueueSize  uint32       `json:"sendQueueSize" toml:"sendQueueSize" yaml:"sendQueueSize"`    // 发送队列大小, 仅发送模式有效, 单位为条数, 默认值1024, 必须为2的幂
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
					if anyMap, ok := item.Item.(map[string]any); ok {
						var natscoreConf conf.NatsCoreConfig
						_ = mapstructure.Decode(anyMap, &natscoreConf)
						item.Item = &natscoreConf
					}
				}
			case "natsjsmq":
				{
					if anyMap, ok := item.Item.(map[string]any); ok {
						var natsjsConf conf.NatsJsConfig
						_ = mapstructure.Decode(anyMap, &natsjsConf)
						natsjsConf.ConsumerConfig.StartWithTimestamp = -1 // 默认值设置为-1, 防御
						if natsjsConf.ConsumerConfig != nil && natsjsConf.ConsumerConfig.AutoCommit == "" {
							natsjsConf.ConsumerConfig.AutoCommit = "native"
						}

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
						// 	fmt.Printf("natsjs config mapstructure err: %v\n", err)
						// }
						// str := kobjs.ObjectToJson5WithoutFunc(natsjsConf)
						// fmt.Printf("%s\n", str)

						item.Item = &natsjsConf
					}
				}
			case "kafkamq":
				{
					if anyMap, ok := item.Item.(map[string]any); ok {
						var kafkaConf conf.KafkaConfig
						_ = mapstructure.Decode(anyMap, &kafkaConf)
						if kafkaConf.Consumer != nil {
							if kafkaConf.Consumer.AutoCommit == "" {
								kafkaConf.Consumer.AutoCommit = "native"
							}

							if kafkaConf.Consumer.AutoCommit == "native" && kafkaConf.Consumer.AutoCommitInterval == 0 {
								kafkaConf.Consumer.AutoCommitInterval = 5000 // 默认 5000ms
							}
						}
						// str := kobjs.ObjectToJson5WithoutFunc(kafkaConf)
						// fmt.Println(str)
						item.Item = &kafkaConf
					}

					//if nil != item.Item.(*KafkaConfig).Net {}
				}
			case "rabbitmq":
				{
					if anyMap, ok := item.Item.(map[string]any); ok {
						var rabbitConf conf.RabbitConfig
						_ = mapstructure.Decode(anyMap, &rabbitConf)
						if rabbitConf.Consumer != nil && rabbitConf.Consumer.AutoCommit == "" {
							rabbitConf.Consumer.AutoCommit = "native"
						}
						item.Item = &rabbitConf
					}

				}
			case "rocketmq":
				{
					if anyMap, ok := item.Item.(map[string]any); ok {
						var rocketConfig conf.RocketConfig
						_ = mapstructure.Decode(anyMap, &rocketConfig)
						if rocketConfig.Consumer != nil && rocketConfig.Consumer.AutoCommit == "" {
							rocketConfig.Consumer.AutoCommit = "native"
						}
						item.Item = &rocketConfig
					}
				}
			case "redismq":
				{
					if anyMap, ok := item.Item.(map[string]any); ok {
						var redisConf conf.RedisConfig
						_ = mapstructure.Decode(anyMap, &redisConf)
						item.Item = &redisConf
					}

				}
			case "mqtt3":
				{
					if anyMap, ok := item.Item.(map[string]any); ok {
						var mqttConf conf.MqttConfig
						_ = mapstructure.Decode(anyMap, &mqttConf)
						item.Item = &mqttConf
					}
				}
			default:
				{
					fmt.Printf("mq source type %s not support", item.MQType)
				}
			}

		}
	}

	for _, item := range that.Target {
		if nil != item && nil != item.Item {
			switch item.MQType {
			case "natscoremq":
				{
					if anyMap, ok := item.Item.(map[string]any); ok {
						var natscoreConf conf.NatsCoreConfig
						_ = mapstructure.Decode(anyMap, &natscoreConf)
						item.Item = &natscoreConf
					}
				}
			case "natsjsmq":
				{
					if anyMap, ok := item.Item.(map[string]any); ok {
						var natsjsConf conf.NatsJsConfig
						_ = mapstructure.Decode(anyMap, &natsjsConf)
						item.Item = &natsjsConf
					}
				}
			case "kafkamq":
				{
					if anyMap, ok := item.Item.(map[string]any); ok {
						var kafkaConf conf.KafkaConfig
						_ = mapstructure.Decode(anyMap, &kafkaConf)
						item.Item = &kafkaConf
					}
				}
			case "rabbitmq":
				{
					if anyMap, ok := item.Item.(map[string]any); ok {
						var rabbitConf conf.RabbitConfig
						_ = mapstructure.Decode(anyMap, &rabbitConf)
						item.Item = &rabbitConf
					}
				}
			case "rocketmq":
				{
					if anyMap, ok := item.Item.(map[string]any); ok {
						var rocketConfig conf.RocketConfig
						_ = mapstructure.Decode(anyMap, &rocketConfig)
						item.Item = &rocketConfig
					}
				}
			case "redismq":
				{
					if anyMap, ok := item.Item.(map[string]any); ok {
						var redisConf conf.RedisConfig
						_ = mapstructure.Decode(anyMap, &redisConf)
						item.Item = &redisConf
					}
				}
			case "mqtt3":
				{
					if anyMap, ok := item.Item.(map[string]any); ok {
						var mqttConf conf.MqttConfig
						_ = mapstructure.Decode(anyMap, &mqttConf)
						item.Item = &mqttConf
					}
				}
			default:
				{
					fmt.Printf("mq target type %s not support", item.MQType)
				}
			}

		}
	}

	if that.SendQueueSize < 1 {
		that.SendQueueSize = 1 * 1024
	}

	if that.Log != nil {
		if that.Log.FlushInterval < 1 {
			that.Log.FlushInterval = 1000
		}

		if that.Log.BufferSize < 1 {
			that.Log.BufferSize = 4 * 1024 * 1024
		}
	}
}
