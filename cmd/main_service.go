package main

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/khan-lau/kmq/example/bean/config"
	"github.com/khan-lau/kmq/example/bean/mq"
	"github.com/khan-lau/kmq/example/service/dispatch"
	"github.com/khan-lau/kmq/example/service/mq/source"
	"github.com/khan-lau/kmq/example/service/mq/target"
	"github.com/khan-lau/kmq/kafka"
	"github.com/khan-lau/kmq/nats"
	"github.com/khan-lau/kmq/rabbitmq"
	"github.com/khan-lau/kmq/rocketmq"
	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/container/klists"
	"github.com/khan-lau/kutils/container/kstrings"
	"github.com/khan-lau/kutils/data"
	"github.com/khan-lau/kutils/filesystem"
	klog "github.com/khan-lau/kutils/klogger"
)

func startMqSource(ctx *kcontext.ContextNode, toHex bool, sourceItems []*config.MQItemObj, offsetSync *mq.OffsetSync, logf klog.AppLogFuncWithTag) {
	waitGroup := sync.WaitGroup{}
	for _, item := range sourceItems {
		waitGroup.Add(1)
		func(item *config.MQItemObj) {
			switch item.MQType {

			case "natscoremq":
				if natsCoreConfig, ok := item.Item.(*config.NatsCoreConfig); ok {
					natsCoreMq, err := source.NewNatsCoreMQ(ctx, "NatsCoreSource", natsCoreConfig, logf)
					if err != nil && logf != nil {
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "create nats core mq source failed, {}", err.Error())
					} else {
						glog.I("start nats core source: [{}], topic: [{}]", strings.Join(natsCoreConfig.BrokerList, ", "), strings.Join(natsCoreConfig.Topics, ", "))

						natsCoreMq.SetOnRecivedCallback(
							func(origin interface{}, name string, topic string, partition int, offset int64, properties map[string]string, message []byte) {
								onRecved(origin, name, topic, partition, offset, properties, item.Compress, toHex, false, message)
							},
						)
						gMqSourceManager[item.MQType] = natsCoreMq
						go func(natsCoreMq *source.NatsCoreMQ) {
							err := natsCoreMq.Start()
							if err != nil && logf != nil {
								logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "start nats core mq source failed, {}", err.Error())
							}
						}(natsCoreMq)
					}
				} else {
					if logf != nil {
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "nats core source config is invalid")
					}
				}

			case "natsjsmq":
				if natsJsConfig, ok := item.Item.(*config.NatsJsConfig); ok {
					// 载入topic offset
					if kafkaOffset, ok := offsetSync.Records[item.MQType]; ok {
					NATS_JS_END_LOOP:
						for _, topic := range natsJsConfig.Topics {
							if topicOffset, ok := kafkaOffset[topic]; ok {
								for _, offset := range topicOffset {
									natsJsConfig.ConsumerConfig.StartWithTimestamp = int64(offset)
									break NATS_JS_END_LOOP
								}
							}
						}
					}
					natsJsMq, err := source.NewNatsJetStreamMQ(ctx, "NatsJSSource", natsJsConfig, logf)
					if err != nil && logf != nil {
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "create nats jetstream mq source failed, {}", err.Error())
					} else {
						glog.I("start nats jetstream source: [{}], topic: [{}]", strings.Join(natsJsConfig.BrokerList, ", "), strings.Join(natsJsConfig.Topics, ", "))
						natsJsMq.SetOnRecivedCallback(
							func(origin interface{}, name string, topic string, partition int, offset int64, properties map[string]string, message []byte) {
								onRecved(origin, name, topic, partition, offset, properties, item.Compress, toHex, natsJsConfig.ConsumerConfig.AutoCommit, message)
							},
						)
						gMqSourceManager[item.MQType] = natsJsMq
						go func(natsJsMq *source.NatsJetStreamMQ) {
							err := natsJsMq.Start()
							if err != nil && logf != nil {
								logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "start nats jetstream mq source failed, {}", err.Error())
							}
						}(natsJsMq)
					}

				} else {
					if logf != nil {
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "nats jetstream source config is invalid")
					}
				}

			case "kafkamq":
				if kafkaConfig, ok := item.Item.(*config.KafkaConfig); ok {
					// 载入topic offset
					if kafkaOffset, ok := offsetSync.Records[item.MQType]; ok {
						for _, topic := range kafkaConfig.Consumer.Topics {
							if topicOffset, ok := kafkaOffset[topic.Name]; ok {
								for partition, offset := range topicOffset {
									if partitionVal, err := strconv.Atoi(partition); err == nil {
										topic.Partition = partitionVal
									} else {
										topic.Partition = 0
									}
									topic.Offset = offset
								}
							}
						}
					}

					kafkaMq, err := source.NewKafkaMQ(ctx, "KafkaSource", kafkaConfig, logf)
					if err != nil && logf != nil {
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "create kafka mq source failed, {}", err.Error())
					} else {
						glog.I("start kafka source {}, topic: {}", kafkaConfig.BrokerList, kafkaConfig.Consumer.Topics)

						kafkaMq.SetOnRecivedCallback(
							func(origin interface{}, name string, topic string, partition int, offset int64, properties map[string]string, message []byte) {
								onRecved(origin, name, topic, partition, offset, properties, item.Compress, toHex, kafkaConfig.Consumer.AutoCommit, message)
							},
						)
						gMqSourceManager[item.MQType] = kafkaMq
						go func(kafkaMq *source.KafkaMQ) {
							err := kafkaMq.Start()
							if err != nil && logf != nil {
								logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "start kafka mq source failed, {}", err.Error())
							}
						}(kafkaMq)
					}
				} else {
					if logf != nil {
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "kafka source config is invalid")
					}
				}

			case "rabbitmq":
				if rabbitConfig, ok := item.Item.(*config.RabbitConfig); ok {
					// 不支持指定 topic offset
					rabbitMq, err := source.NewRabbitMQ(ctx, "RabbitSource", rabbitConfig, logf)
					if err != nil && logf != nil {
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "create rabbit mq source failed, {}", err.Error())
					} else {
						glog.I("start rabbitmq source, {}:{}, exchange: {}, queue: {}", rabbitConfig.Host, int(rabbitConfig.Port), rabbitConfig.Consumer.Exchange, rabbitConfig.Consumer.QueueName)

						rabbitMq.SetOnRecivedCallback(
							func(origin interface{}, name string, topic string, partition int, offset int64, properties map[string]string, message []byte) {
								onRecved(origin, name, topic, partition, offset, properties, item.Compress, toHex, rabbitConfig.Consumer.AutoCommit, message)
							},
						)
						gMqSourceManager[item.MQType] = rabbitMq
						go func(rabbitMq *source.RabbitMQ) {
							err := rabbitMq.Start()
							if err != nil && logf != nil {
								logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "start rabbit mq source failed, {}", err.Error())
							}
						}(rabbitMq)
					}
				} else {
					if logf != nil {
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "rabbit source config is invalid")
					}
				}

			case "redismq":
				if redisConfig, ok := item.Item.(*config.RedisConfig); ok {
					// 不支持offset 订阅
					redisMq, err := source.NewRedisMQ(ctx, "RedisSource", redisConfig, logf)
					if err != nil && logf != nil {
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "create redis mq source failed, {}", err.Error())
					} else {
						glog.I("start redis source, {}:{}, topic: {}", redisConfig.Host, int(redisConfig.Port), redisConfig.Topics)
						redisMq.SetOnRecivedCallback(
							func(origin interface{}, name string, topic string, _ int, _ int64, _ map[string]string, message []byte) {
								onRecved(origin, name, topic, 0, 0, nil, item.Compress, toHex, false, message)
							},
						)
						gMqSourceManager[item.MQType] = redisMq
						go func(redisMq *source.RedisMQ) {
							err := redisMq.Start()
							if err != nil && logf != nil {
								logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "start redis mq source failed, {}", err.Error())
							}
						}(redisMq)
					}
				} else {
					if logf != nil {
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "redis source config is invalid")
					}
				}

			case "rocketmq":
				{
					if rocketConfig, ok := item.Item.(*config.RocketConfig); ok {
						// 载入topic offset
						if rocketOffset, ok := offsetSync.Records[item.MQType]; ok {
						ROCKET_END_LOOP:
							for _, topic := range rocketConfig.Consumer.Topics {
								if topicOffset, ok := rocketOffset[topic]; ok {
									for _, offset := range topicOffset {
										rocketConfig.Consumer.Timestamp = kstrings.Sprintf("{}", offset)
										break ROCKET_END_LOOP
									}
								}
							}
						}
						rocketMq, err := source.NewRocketMQ(ctx, "RocketSource", rocketConfig, logf)
						if err != nil && logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "create rocket mq source failed, {}", err.Error())
						} else {
							glog.I("start rocketmq source: {}, topic: {}", rocketConfig.Servers, rocketConfig.Consumer.Topics)
							rocketMq.SetOnRecivedCallback(
								func(origin interface{}, name string, topic string, partition int, offset int64, properties map[string]string, message []byte) {
									onRecved(origin, name, topic, partition, offset, properties, item.Compress, toHex, rocketConfig.Consumer.AutoCommit, message)
								},
							)
							gMqSourceManager[item.MQType] = rocketMq
							go func(rocketMq *source.RocketMQ) {
								err := rocketMq.Start()
								if err != nil && logf != nil {
									logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "start rocket mq source failed, {}", err.Error())
								}
							}(rocketMq)
						}
					} else {
						if logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "rocket source config is invalid")
						}
					}
				}

			case "mqtt3":
				{
					if mqttConfig, ok := item.Item.(*config.MqttConfig); ok {

						mqttClient, err := source.NewMqttMQ(ctx, "MqttSource", mqttConfig, logf)
						if err != nil && logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "create rocket mq source failed, {}", err.Error())
						} else {
							glog.I("start rocketmq source: {}, topic: {}", mqttConfig.Broker, strings.Join(mqttConfig.Topics, ","))
							mqttClient.SetOnRecivedCallback(
								func(origin interface{}, name string, topic string, partition int, offset int64, properties map[string]string, message []byte) {
									onRecved(origin, name, topic, 0, 0, nil, item.Compress, toHex, false, message)
								},
							)
							gMqSourceManager[item.MQType] = mqttClient
							go func(mqtt *source.MqttMQ) {
								err := mqtt.Start()
								if err != nil && logf != nil {
									logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "start rocket mq source failed, {}", err.Error())
								}
							}(mqttClient)
						}
					} else {
						if logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "rocket source config is invalid")
						}
					}
				}

			default:
				if logf != nil {
					logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "unknown mq source type: {}", item.MQType)
				}
			}
			waitGroup.Done()
		}(item)
	}

	waitGroup.Wait()
}

func startMqTarget(ctx *kcontext.ContextNode, targetItems []*config.MQItemObj, logf klog.AppLogFuncWithTag) {
	waitGroup := sync.WaitGroup{}

	for _, item := range targetItems {
		waitGroup.Add(1)
		func(item *config.MQItemObj) {
			switch item.MQType {
			case "natscoremq":
				if natsCoreConfig, ok := item.Item.(*config.NatsCoreConfig); ok {
					natsCoreMq, err := target.NewNatsCoreMQ(ctx, "NatsCoreTarget", natsCoreConfig, logf)
					if err != nil && logf != nil {
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "create nats core mq target failed, {}", err.Error())
					} else {
						glog.I("start nats core target: [{}], topic: [{}]", strings.Join(natsCoreConfig.BrokerList, ", "), strings.Join(natsCoreConfig.Topics, ", "))

						gMqTargetManager[item.MQType] = natsCoreMq
						go func(natsCore *target.NatsCoreMQ) {
							err := natsCore.Start()
							if err != nil && logf != nil {
								logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "start nats core mq target failed, {}", err.Error())
							}
						}(natsCoreMq)
					}
				} else {
					if logf != nil {
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "nats core target config is invalid")
					}
				}

			case "natsjsmq":
				if natsJsConfig, ok := item.Item.(*config.NatsJsConfig); ok {
					natsJsMq, err := target.NewNatsJetStreamMQ(ctx, "NatsJSTarget", natsJsConfig, logf)
					if err != nil && logf != nil {
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "create nats jetstream mq target failed, {}", err.Error())
					} else {
						glog.I("start nats jetstream target: [{}], topic: [{}]", strings.Join(natsJsConfig.BrokerList, ", "), strings.Join(natsJsConfig.Topics, ", "))

						gMqTargetManager[item.MQType] = natsJsMq
						go func(natsJs *target.NatsJetStreamMQ) {
							err := natsJs.Start()
							if err != nil && logf != nil {
								logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "start nats jetstream mq target failed, {}", err.Error())
							}
						}(natsJsMq)
					}

				} else {
					if logf != nil {
						// logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "nats jetstream target config is invalid, %#v", item.Item)
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "nats jetstream target config is invalid")
					}
				}

			case "kafkamq":
				if kafkaConfig, ok := item.Item.(*config.KafkaConfig); ok {
					kafkaMq, err := target.NewKafkaMQ(ctx, "KafkaTarget", kafkaConfig, logf)
					if err != nil && logf != nil {
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "create kafka mq target failed, {}", err.Error())
					} else {
						glog.I("start kafka target: {}, topic: {}", kafkaConfig.BrokerList, kafkaConfig.Producer.Topics)
						gMqTargetManager[item.MQType] = kafkaMq
						go func(kafkaMq *target.KafkaMQ) {
							err := kafkaMq.Start()
							if err != nil && logf != nil {
								logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "start kafka mq target failed, {}", err.Error())
							}
						}(kafkaMq)
					}
				} else {
					if logf != nil {
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "kafka target config is invalid")
					}
				}
			case "rabbitmq":
				if rabbitConfig, ok := item.Item.(*config.RabbitConfig); ok {
					rabbitMq, err := target.NewRabbitMQ(ctx, "RabbitTarget", rabbitConfig, logf)
					if err != nil && logf != nil {
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "create rabbit mq target failed, {}", err.Error())
					} else {
						glog.I("start rabbitmq target, {}:{}, route: {}", rabbitConfig.Host, int(rabbitConfig.Port), rabbitConfig.Producer.Router)
						gMqTargetManager[item.MQType] = rabbitMq
						go func(rabbitMq *target.RabbitMQ) {
							err := rabbitMq.Start()
							if err != nil && logf != nil {
								logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "start rabbit mq target failed, {}", err.Error())
							}
						}(rabbitMq)
					}
				} else {
					if logf != nil {
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "rabbit target config is invalid")
					}
				}
			case "redismq":
				if redisConfig, ok := item.Item.(*config.RedisConfig); ok {
					redisMq, err := target.NewRedisMQ(ctx, "RedisTarget", redisConfig, logf)
					if err != nil && logf != nil {
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "create redis mq target failed, {}", err.Error())
					} else {
						glog.I("start redismq target, {}:{}, topic: {}", redisConfig.Host, int(redisConfig.Port), redisConfig.Topics)
						gMqTargetManager[item.MQType] = redisMq
						go func(redisMq *target.RedisMQ) {
							err := redisMq.Start()
							if err != nil && logf != nil {
								logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "start redis mq target failed, {}", err.Error())
							}
						}(redisMq)
					}
				} else {
					if logf != nil {
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "redis target config is invalid")
					}
				}
			case "rocketmq":
				if rocketConfig, ok := item.Item.(*config.RocketConfig); ok {
					rocketMq, err := target.NewRocketMQ(ctx, "RocketTarget", rocketConfig, logf)
					if err != nil && logf != nil {
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "create rocket mq target failed, {}", err.Error())
					} else {
						glog.I("start rocketmq target, {}, topic: {}", rocketConfig.Servers, rocketConfig.Producer.Topics)
						gMqTargetManager[item.MQType] = rocketMq
						go func(rocketMq *target.RocketMQ) {
							err := rocketMq.Start()
							if err != nil && logf != nil {
								logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "start rocket mq target failed, {}", err.Error())
							}
						}(rocketMq)
					}
				} else {
					if logf != nil {
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "rocket target config is invalid")
					}
				}
			case "mqtt3":
				if mqttConfig, ok := item.Item.(*config.MqttConfig); ok {
					mqttClient, err := target.NewMqttMQ(ctx, "MqttSource", mqttConfig, logf)
					if err != nil && logf != nil {
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "create rocket mq target failed, {}", err.Error())
					} else {
						glog.I("start rocketmq target, {}, topic: {}", mqttConfig.Broker, strings.Join(mqttConfig.Topics, ","))
						gMqTargetManager[item.MQType] = mqttClient
						go func(mqtt *target.MqttMQ) {
							err := mqtt.Start()
							if err != nil && logf != nil {
								logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "start rocket mq target failed, {}", err.Error())
							}
						}(mqttClient)
					}
				} else {
					if logf != nil {
						logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "rocket target config is invalid")
					}
				}
			default:
				if logf != nil {
					logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "unknown mq target type: {}", item.MQType)
				}
			}
			waitGroup.Done()
		}(item)
	}

	waitGroup.Wait()
}

func stopMqSourceManager() {
	for _, v := range gMqSourceManager {
		_ = v.Stop()
	}
}

func stopMqTargetManager() {
	for _, v := range gMqTargetManager {
		_ = v.Stop()
	}
}

func loadOffsetCache(conf *config.Configure) *mq.OffsetSync {
	offsetSync := mq.NewOffsetSync(conf.SyncTime, conf.SyncFile, LogFunc)
	// 载入本地缓存文件
	if filesystem.IsFileExists(conf.SyncFile) {

		if buf, err := os.ReadFile(conf.SyncFile); err != nil {
			glog.E("read file {} error: {}", conf.SyncFile, err)
		} else {
			tmap := map[string]map[string]map[string]int64{}
			if err = json.Unmarshal(buf, &tmap); err != nil {
				glog.E("unmarshal file {} error: {}", conf.SyncFile, err)
			} else {
				catcheOffset := offsetSync.Records
				for mqType, topicMap := range tmap {
					catcheOffset[mqType] = map[string]map[string]int64{}
					for topic, partitionOffset := range topicMap {
						catcheOffset[mqType][topic] = map[string]int64{}
						for part, offset := range partitionOffset {
							glog.I("load offset: mqType:{} topic:{} partition{}: offset:{}", mqType, topic, part, offset)
							catcheOffset[mqType][topic][part] = offset
						}
					}
				}
			}
		}
	}
	return offsetSync
}

///////////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////////////

// type GenericMessage struct {
// 	Topic   string
// 	Message string
// }

/* 解析重放消息文件 */
func getReplayData(toHex bool, path string) *klists.KList[*dispatch.GenericMessage] {
	messages := klists.New[*dispatch.GenericMessage]()

	glog.Info("Server parse file: %s", path)
	file, err := os.Open(path)
	if err != nil {
		glog.Error("Open file: %s error: %s", path, err)
		return nil
	}
	defer file.Close()

	fileInfo, err := os.Stat(path)
	if err != nil {
		glog.Error("file %s not found", path)
		return nil
	}

	length := fileInfo.Size()
	// reader := bufio.NewReaderSize(file, 5*1024*1024) // 缓冲区
	reader := bufio.NewReaderSize(file, 64*1024) // 缓冲区

	if length < 1 {
		glog.Error("parse test.message error, document is empty")
		return nil
	}

	var firstErr error = nil
	var header string = ""
	record_count := int64(0)
	auto_exit := false

	for {
		firstErr = nil
		line, err := reader.ReadString('\n')
		if nil != err && len(line) == 0 {
			// firstErr = err
			break
		}

		line = strings.TrimSpace(line)
		//忽略行注释 与 空行
		if strings.HasPrefix(line, "//") || strings.HasPrefix(line, "#") || len(line) == 0 {
			continue
		}

		// 处理 header
		if strings.HasPrefix(line, "test.message") {
			// 一个document只允许一个 header
			if header == "" {
				header = line
				continue
			} else {
				firstErr = fmt.Errorf("parse test.message error, document has many header")
				break
			}
		}

		if header == "" {
			firstErr = fmt.Errorf("parse test.message error error, document header not at first")
			break
		}

		// element 开始
		pos := strings.Index(line, " ")
		message := ""
		topic := ""
		if pos > -1 {
			topic = line[:pos]

			rawMessage := line[pos+1:]
			trimMessage := strings.TrimSpace(rawMessage)
			msgLength := len(trimMessage)
			if trimMessage[msgLength-1] != '"' && trimMessage[msgLength-1] != '\'' {
				// 如果行尾包含注释, 则去掉注释部分
				commentPos := strings.LastIndex(trimMessage, "#")
				if commentPos > -1 {
					trimMessage = trimMessage[:commentPos]
				}
				commentPos = strings.LastIndex(trimMessage, "//")
				if commentPos > -1 {
					trimMessage = trimMessage[:commentPos]
				}
			}

			message = strings.TrimFunc(trimMessage, func(r rune) bool {
				return r == '\'' || r == '"'
			})
			message = strings.Replace(message, "\\\"", "\"", -1)
			message = strings.Replace(message, "\\\\u", "\\u", -1)
			arr := strings.Split(message, ",")
			record_count += int64(len(arr))

			dataStr := []byte(message)
			if toHex {
				if tmpData, err := hex.DecodeString(message); err == nil {
					dataStr = tmpData
				} else {
					glog.E("hex decode line: {} error: {}", line, err.Error())
					continue
				}
			}

			messages.PushBack(&dispatch.GenericMessage{Topic: topic, Message: dataStr})
		} else {
			if line == "quit" || line == "exit" || line == "stop" || line == "QUIT" || line == "EXIT" || line == "STOP" {
				messages.PushBack(&dispatch.GenericMessage{Topic: "quit", Message: []byte("")})
				auto_exit = true
			} else {
				glog.Error("Error data: %s", line)
				continue
			}
		}
	}

	if nil != firstErr {
		glog.Error("Error: %s", firstErr.Error())
		return nil
	}

	if messages.Len() > 0 {
		if auto_exit {
			glog.Info("parse %s success, message count: %d, records: %d", path, messages.Len()-1, record_count)
		} else {
			glog.Info("parse %s success, message count: %d, records: %d", path, messages.Len(), record_count)
		}
	}

	return messages
}

func generalMessage(handler *dispatch.DispatchService, resetTimestamp bool, message *dispatch.GenericMessage) {
	content := string(message.Message)
	if resetTimestamp {
		// 不是JSON
		if !strings.HasPrefix(content, "{") && !strings.HasPrefix(content, "[") {
			// 一条消息中包含多条记录, 每条记录都需要重置时间戳
			records := strings.Split(content, ",")
			if len(records) > 0 {
				distRecord := make([]string, 0, len(records))
				for _, record := range records {
					record := kstrings.TrimSpace(record)
					tmpArr := strings.Split(record, "@")
					if len(tmpArr) == 2 {
						pointName := tmpArr[0]
						originData := tmpArr[1]
						tmpArr2 := strings.Split(originData, ":")
						if len(tmpArr2) >= 3 {

							timestamp, _ := strconv.ParseInt(tmpArr2[2], 10, 64)
							newTimestamp := time.Now().Unix()
							if timestamp > 9999999999 { // 根据原来记录中的时间戳的精度确定新的时间戳用秒还是毫秒
								newTimestamp = time.Now().UnixMilli()
							}
							tmpArr2[2] = fmt.Sprintf("%d", newTimestamp)
							dataStr := strings.Join(tmpArr2, ":")
							record = fmt.Sprintf("%s@%s", pointName, dataStr)
							distRecord = append(distRecord, record)
						}
					}
				}
				content = strings.Join(distRecord, ",")
			}
		}
	}

	if len(message.Message) < 1 || len(message.Topic) < 1 {
		glog.W("{}", "Message or topic is empty")
	}
	handler.DoSend(&dispatch.GenericMessage{Topic: message.Topic, Message: []byte(content)})
}

///////////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////////////

// onBussDataRecved 从上游MQ收到的业务数据
//
// 参数:
//
//   - name string: 数据源名称
//   - topic string: 主题名称
//   - partition int: 分区编号
//   - offset int64: 偏移量
//   - properties map[string]string: 标签信息
//   - isCompress bool: 是否为压缩数据
//   - message []byte: 消息数据
func onRecved(origin any, name string, topic string, partition int, offset int64, _ map[string]string, isCompress bool, toHex bool, autoAck bool, message []byte) {
	// 源数据是否为压缩数据, 压缩数据必须是zip压缩算法
	str := ""
	if isCompress {
		buf, err := data.UnZip(message)
		if err != nil {
			glog.E("onRecved: name={}, topic={}, partition={}, offset={}, uncompress error= {}", name, topic, partition, offset, err)
			return
		}
		str = string(buf)
	} else {
		str = string(message)
	}

	//// TODO 解析从消息中获取需要下发的topic 和 payload, 下发给下游MQ处理
	// flag := publish(to, topic, []byte(str), nil)
	// if !flag {
	// 	glog.E("onRecived: name={}, topic={}, partition={}, offset={}, publish error= {}", name, topic, partition, offset, "sent to target faulted")
	// }

	var err error
	switch t := origin.(type) {
	case *rabbitmq.Message:
		if !autoAck {
			err = t.Ack(false) // false: 只确认当前这条消息; true: 批量确认 DeliveryTag <= current DeliveryTag 的所有消息
		}

	case *rocketmq.Message:
		if !autoAck {
			err = t.Ack() // 批量确认
		}
	case *nats.NatsMessage:
		if !autoAck {
			err = t.Ack() // 如果想批量确认 需要将 AckPolicy设置为 `AckAllPolicy`
		}
	case *kafka.KafkaMessage:
		if !autoAck {
			err = t.Ack() // 确认当前消息seq之前的所有消息
		}
	case nil:
		// 不支持ack的MQ 直接忽略
	default:
		// 其他
	}

	if err == nil {
		dataStr := str
		if toHex {
			dataStr = hex.EncodeToString([]byte(str))
		}
		glog.D("onRecived: name={}, topic={}, partition={}, offset={}, message={}", name, topic, partition, offset, dataStr)
		switch name {
		case "KafkaSource":
			gOffsetSync.Set("kafkamq", topic, strconv.Itoa(partition), offset)
		case "RocketSource":
			gOffsetSync.Set("rocketmq", topic, strconv.Itoa(partition), offset)
		case "NatsJSSource":
			gOffsetSync.Set("natsjsmq", topic, strconv.Itoa(partition), offset)
		}
	} else {
		glog.E("onRecived: name={}, topic={}, partition={}, offset={}, ack return error={}", name, topic, partition, offset, err)
	}
}
