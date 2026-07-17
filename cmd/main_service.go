package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/khan-lau/kmq-utils/kafkamq"
	"github.com/khan-lau/kmq-utils/natsmq"
	"github.com/khan-lau/kmq-utils/rabbitmq"
	"github.com/khan-lau/kmq-utils/rocketmq"
	"github.com/khan-lau/kmq/internal/config"
	"github.com/khan-lau/kmq/service/idl"
	mqConf "github.com/khan-lau/kmq/service/mq/config"
	"github.com/khan-lau/kmq/service/mq/offset"
	"github.com/khan-lau/kmq/service/mq/router"
	"github.com/khan-lau/kmq/service/mq/source"
	"github.com/khan-lau/kmq/service/mq/target"
	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/container/kstrings"
	"github.com/khan-lau/kutils/data"
	"github.com/khan-lau/kutils/filesystem"
	klog "github.com/khan-lau/kutils/klogger"
	"github.com/khan-lau/kutils/ksync"
	"github.com/khan-lau/kutils/kuuid"
)

// startMqSource 启动MQ源
//
// 参数:
//   - ctx: 上下文对象
//   - recvQueueSize: 接收队列大小, 2的次幂; 2 4 8 16 32 ....
//   - toHex: 是否十六进制格式
//   - sourceItems: MQ配置项列表
//   - offsetSync: 偏移量同步对象
//   - logf: 日志记录函数
func startMqSource(ctx *kcontext.ContextNode, recvQueueSize uint, toHex bool, sourceItems []*config.MQItemObj, offsetSync *offset.OffsetSync, logf klog.AppLogFuncWithTag) {
	waitGroup := sync.WaitGroup{}

	for _, item := range sourceItems {
		waitGroup.Add(1)
		func(item *config.MQItemObj) {
			defer waitGroup.Done()

			switch item.MQType {
			case "natscoremq":
				{
					if natsCoreConfig, ok := item.Item.(*mqConf.NatsCoreConfig); ok {
						natsCoreMq, err := source.NewNatsCoreMQ(ctx, "NatsCoreSource", natsCoreConfig, recvQueueSize, logf)
						if err != nil && logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "create nats core mq source failed, %s", err.Error())
						} else {
							if logf != nil {
								logf(klog.InfoLevel, DEFAULT_LOGGER_TAG, 0, "start nats core source: [%s], topic: [%s]", strings.Join(natsCoreConfig.BrokerList, ", "), strings.Join(natsCoreConfig.Topics, ", "))
							}

							natsCoreMq.SetOnRecivedCallback(
								func(origin any, name string, topic string, partition int, offset int64, properties map[string]string, message []byte) {
									onRecved(origin, name, topic, partition, offset, properties, item.Compress, toHex, false, message)
								},
							)
							gMqSourceManager[item.MQType] = natsCoreMq
							go func(natsCoreMq *source.NatsCoreMQ) {
								err := natsCoreMq.Start()
								if err != nil && logf != nil {
									logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "start nats core mq source failed, %s", err.Error())
								}
							}(natsCoreMq)
						}
					} else {
						if logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "nats core source config is invalid")
						}
					}
				}

			case "natsjsmq":
				{
					if natsJsConfig, ok := item.Item.(*mqConf.NatsJsConfig); ok {
						// 载入topic offset
						if offsetSync != nil {
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
						}
						natsJsMq, err := source.NewNatsJetStreamMQ(ctx, "NatsJSSource", natsJsConfig, recvQueueSize, logf)
						if err != nil && logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "create nats jetstream mq source failed, %s", err.Error())
						} else {
							if logf != nil {
								logf(klog.InfoLevel, DEFAULT_LOGGER_TAG, 0, "start nats jetstream source: [%s], topic: [%s]", strings.Join(natsJsConfig.BrokerList, ", "), strings.Join(natsJsConfig.Topics, ", "))
							}
							natsJsMq.SetOnRecivedCallback(
								func(origin any, name string, topic string, partition int, offset int64, properties map[string]string, message []byte) {
									onRecved(origin, name, topic, partition, offset, properties, item.Compress, toHex, natsJsConfig.ConsumerConfig.AutoCommit == natsmq.AUTO_COMMIT_NONE, message)
								},
							)
							gMqSourceManager[item.MQType] = natsJsMq
							go func(natsJsMq *source.NatsJetStreamMQ) {
								err := natsJsMq.Start()
								if err != nil && logf != nil {
									logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "start nats jetstream mq source failed, %s", err.Error())
								}
							}(natsJsMq)
						}

					} else {
						if logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "nats jetstream source config is invalid")
						}
					}
				}

			case "kafkamq":
				{
					if kafkaConfig, ok := item.Item.(*mqConf.KafkaConfig); ok {
						// 载入topic offset
						if offsetSync != nil {
							if kafkaOffset, ok := offsetSync.Records[item.MQType]; ok {
								for _, topic := range kafkaConfig.Consumer.Topics {
									if topicOffset, ok := kafkaOffset[topic.Name]; ok {
										for partition, offset := range topicOffset {
											if partitionVal, err := strconv.Atoi(partition); err == nil {
												if pos := slices.IndexFunc(topic.Partitions, func(partition *mqConf.Partition) bool { return partition.Partition == partitionVal }); pos >= 0 {
													topic.Partitions[pos].Offset = offset
												}
											}
										}
									}
								}
							}
						}
						kafkaMq, err := source.NewKafkaMQ(ctx, "KafkaSource", kafkaConfig, recvQueueSize, logf)
						if err != nil && logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "create kafka mq source failed, %s", err.Error())
						} else {
							if logf != nil {
								logf(klog.InfoLevel, DEFAULT_LOGGER_TAG, 0, "start kafka source [%s], topic: [%s]", strings.Join(kafkaConfig.BrokerList, ", "), mqConf.KafkaTopicsToStr(kafkaConfig.Consumer.Topics))
							}

							kafkaMq.SetOnRecivedCallback(
								func(origin any, name string, topic string, partition int, offset int64, properties map[string]string, message []byte) {
									onRecved(origin, name, topic, partition, offset, properties, item.Compress, toHex, kafkaConfig.Consumer.AutoCommit == kafkamq.AUTO_COMMIT_NONE, message)
								},
							)
							gMqSourceManager[item.MQType] = kafkaMq
							go func(kafkaMq *source.KafkaMQ) {
								err := kafkaMq.Start()
								if err != nil && logf != nil {
									logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "start kafka mq source failed, %s", err.Error())
								}
							}(kafkaMq)
						}
					} else {
						if logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "kafka source config is invalid")
						}
					}
				}

			case "rabbitmq":
				{
					if rabbitConfig, ok := item.Item.(*mqConf.RabbitConfig); ok {
						// 不支持指定 topic offset
						rabbitMq, err := source.NewRabbitMQ(ctx, "RabbitSource", rabbitConfig, logf)
						if err != nil && logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "create rabbit mq source failed, %s", err.Error())
						} else {
							if logf != nil {
								logf(klog.InfoLevel, DEFAULT_LOGGER_TAG, 0, "start rabbitmq source, [%s]:%s, exchange: %s, queue: %s", strings.Join(rabbitConfig.Addrs, ", "),
									rabbitConfig.VHost, rabbitConfig.Consumer.Exchange, rabbitConfig.Consumer.QueueName)
							}

							rabbitMq.SetOnRecivedCallback(
								func(origin any, name string, topic string, partition int, offset int64, properties map[string]string, message []byte) {
									onRecved(origin, name, topic, partition, offset, properties, item.Compress, toHex, rabbitConfig.Consumer.AutoCommit == rabbitmq.AUTO_COMMIT_NONE, message)
								},
							)
							gMqSourceManager[item.MQType] = rabbitMq
							go func(rabbitMq *source.RabbitMQ) {
								err := rabbitMq.Start()
								if err != nil && logf != nil {
									logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "start rabbit mq source failed, %s", err.Error())
								}
							}(rabbitMq)
						}
					} else {
						if logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "rabbit source config is invalid")
						}
					}
				}

			case "redismq":
				{
					if redisConfig, ok := item.Item.(*mqConf.RedisConfig); ok {
						// 不支持offset 订阅
						redisMq, err := source.NewRedisMQ(ctx, "RedisSource", redisConfig, recvQueueSize, logf)
						if err != nil && logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "create redis mq source failed, %s", err.Error())
						} else {
							if logf != nil {
								logf(klog.InfoLevel, DEFAULT_LOGGER_TAG, 0, "start redis source, [%s], topic: [%s]", strings.Join(redisConfig.Addrs, ", "), strings.Join(redisConfig.Topics, ", "))
							}
							redisMq.SetOnRecivedCallback(
								func(origin any, name string, topic string, _ int, _ int64, _ map[string]string, message []byte) {
									onRecved(origin, name, topic, 0, 0, nil, item.Compress, toHex, false, message)
								},
							)
							gMqSourceManager[item.MQType] = redisMq
							go func(redisMq *source.RedisMQ) {
								err := redisMq.Start()
								if err != nil && logf != nil {
									logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "start redis mq source failed, %s", err.Error())
								}
							}(redisMq)
						}
					} else {
						if logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "redis source config is invalid")
						}
					}
				}

			case "rocketmq":
				{
					if rocketConfig, ok := item.Item.(*mqConf.RocketConfig); ok {
						// 载入topic offset
						if offsetSync != nil {
							if rocketOffset, ok := offsetSync.Records[item.MQType]; ok {
							ROCKET_END_LOOP:
								for _, topic := range rocketConfig.Consumer.Topics {
									if topicOffset, ok := rocketOffset[topic]; ok {
										for _, offset := range topicOffset {
											rocketConfig.Consumer.Timestamp = fmt.Sprintf("%d", offset)
											break ROCKET_END_LOOP
										}
									}
								}
							}
						}
						rocketMq, err := source.NewRocketMQ(ctx, "RocketSource", rocketConfig, recvQueueSize, logf)
						if err != nil && logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "create rocket mq source failed, %s", err.Error())
						} else {
							if logf != nil {
								logf(klog.InfoLevel, DEFAULT_LOGGER_TAG, 0, "start rocketmq source: [%s], topic: [%s]", strings.Join(rocketConfig.Servers, ", "), strings.Join(rocketConfig.Consumer.Topics, ", "))
							}
							rocketMq.SetOnRecivedCallback(
								func(origin any, name string, topic string, partition int, offset int64, properties map[string]string, message []byte) {
									onRecved(origin, name, topic, partition, offset, properties, item.Compress, toHex, rocketConfig.Consumer.AutoCommit == rocketmq.AUTO_COMMIT_NONE, message)
								},
							)
							gMqSourceManager[item.MQType] = rocketMq
							go func(rocketMq *source.RocketMQ) {
								err := rocketMq.Start()
								if err != nil && logf != nil {
									logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "start rocket mq source failed, %s", err.Error())
								}
							}(rocketMq)
						}
					} else {
						if logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "rocket source config is invalid")
						}
					}
				}

			case "mqtt3":
				{
					if mqttConfig, ok := item.Item.(*mqConf.MqttConfig); ok {

						mqttClient, err := source.NewMqttMQ(ctx, "MqttSource", mqttConfig, recvQueueSize, logf)
						if err != nil && logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "create mqtt3 mq source failed, %s", err.Error())
						} else {
							if logf != nil {
								logf(klog.InfoLevel, DEFAULT_LOGGER_TAG, 0, "start mqtt3 source: %s, topic: [%s]", mqttConfig.Broker, strings.Join(mqttConfig.Topics, ","))
							}
							mqttClient.SetOnRecivedCallback(
								func(origin any, name string, topic string, partition int, offset int64, properties map[string]string, message []byte) {
									onRecved(origin, name, topic, 0, 0, nil, item.Compress, toHex, false, message)
								},
							)
							gMqSourceManager[item.MQType] = mqttClient
							go func(mqtt *source.MqttMQ) {
								err := mqtt.Start()
								if err != nil && logf != nil {
									logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "start mqtt3 source failed, %s", err.Error())
								}
							}(mqttClient)
						}
					} else {
						if logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "mqtt3 source config is invalid")
						}
					}
				}

			default:
				if logf != nil {
					logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "unknown mq source type: %s", item.MQType)
				}
			}
		}(item)
	}

	waitGroup.Wait()
}

// startMqTarget 启动MQ转发目标服务
//
// 参数:
//   - ctx: 上下文对象
//   - sendQueueSize: 发送队列大小, 2的次幂; 2 4 8 16 32 ....
//   - targetItems: MQ配置项列表
//   - countdown: 计数器, 用于计数MQ目标服务的数量
//   - logf: 日志记录函数
func startMqTarget(ctx *kcontext.ContextNode, sendQueueSize uint, targetItems []*config.MQItemObj, countdown *ksync.CountDownLatch, logf klog.AppLogFuncWithTag) {
	waitGroup := sync.WaitGroup{}

	for _, item := range targetItems {
		waitGroup.Add(1)
		func(item *config.MQItemObj) {
			defer waitGroup.Done()

			switch item.MQType {
			case "natscoremq":
				{
					if natsCoreConfig, ok := item.Item.(*mqConf.NatsCoreConfig); ok {
						natsCoreMq, err := target.NewNatsCoreMQ(ctx, "NatsCoreTarget", natsCoreConfig, sendQueueSize, false, logf)
						if err != nil && logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "create nats core mq target failed, %s", err.Error())
						} else {

							natsCoreMq.SetOnReady(func(ready bool) {
								if logf != nil {
									logf(klog.InfoLevel, DEFAULT_LOGGER_TAG, 0, "nats core target ready")
								}
								countdown.CountDown()
							})
							if logf != nil {
								logf(klog.InfoLevel, DEFAULT_LOGGER_TAG, 0, "start nats core target: [%s], topic: [%s]", strings.Join(natsCoreConfig.BrokerList, ", "), strings.Join(natsCoreConfig.Topics, ", "))
							}
							gMqTargetManager[item.MQType] = natsCoreMq
							go func(natsCore *target.NatsCoreMQ) {
								err := natsCore.Start()
								if err != nil && logf != nil {
									logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "start nats core mq target failed, %s", err.Error())
								}
							}(natsCoreMq)
						}
					} else {
						if logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "nats core target config is invalid")
						}
					}
				}

			case "natsjsmq":
				{
					if natsJsConfig, ok := item.Item.(*mqConf.NatsJsConfig); ok {
						natsJsMq, err := target.NewNatsJetStreamMQ(ctx, "NatsJSTarget", natsJsConfig, sendQueueSize, false, logf)
						if err != nil && logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "create nats jetstream mq target failed, %s", err.Error())
						} else {
							if logf != nil {
								logf(klog.InfoLevel, DEFAULT_LOGGER_TAG, 0, "start nats jetstream target: [%s], topic: [%s]", strings.Join(natsJsConfig.BrokerList, ", "), strings.Join(natsJsConfig.Topics, ", "))
							}
							natsJsMq.SetOnReady(func(ready bool) {
								if logf != nil {
									logf(klog.InfoLevel, DEFAULT_LOGGER_TAG, 0, "nats jetstream target ready")
								}
								countdown.CountDown()
							})
							gMqTargetManager[item.MQType] = natsJsMq
							go func(natsJs *target.NatsJetStreamMQ) {
								err := natsJs.Start()
								if err != nil && logf != nil {
									logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "start nats jetstream mq target failed, %s", err.Error())
								}
							}(natsJsMq)
						}

					} else {
						if logf != nil {
							// logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, "nats jetstream target config is invalid, %#v", item.Item)
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "nats jetstream target config is invalid")
						}
					}
				}

			case "kafkamq":
				{
					if kafkaConfig, ok := item.Item.(*mqConf.KafkaConfig); ok {
						// if kafkaConfig.Net.MaxOpenRequests > 1 {
						// 	kafkaConfig.Producer.Idempotent = true
						// }

						kafkaMq, err := target.NewKafkaMQ(ctx, "KafkaTarget", kafkaConfig, sendQueueSize, false, logf)
						if err != nil && logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "create kafka mq target failed, %s", err.Error())
						} else {
							kafkaMq.SetOnReady(func(ready bool) {
								if logf != nil {
									logf(klog.InfoLevel, DEFAULT_LOGGER_TAG, 0, "kafka target ready")
								}
								countdown.CountDown()
							})
							if logf != nil {
								logf(klog.InfoLevel, DEFAULT_LOGGER_TAG, 0, "start kafka target: [%s], topic: [%s]", strings.Join(kafkaConfig.BrokerList, ","), mqConf.KafkaTopicsToStr(kafkaConfig.Producer.Topics))
							}
							gMqTargetManager[item.MQType] = kafkaMq
							go func(kafkaMq *target.KafkaMQ) {
								err := kafkaMq.Start()
								if err != nil && logf != nil {
									logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "start kafka mq target failed, %s", err.Error())
								}
							}(kafkaMq)
						}
					} else {
						if logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "kafka target config is invalid")
						}
					}
				}

			case "rabbitmq":
				{
					if rabbitConfig, ok := item.Item.(*mqConf.RabbitConfig); ok {
						rabbitMq, err := target.NewRabbitMQ(ctx, "RabbitTarget", rabbitConfig, sendQueueSize, false, logf)
						if err != nil && logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "create rabbit mq target failed, %s", err.Error())
						} else {
							rabbitMq.SetOnReady(func(ready bool) {
								if logf != nil {
									logf(klog.InfoLevel, DEFAULT_LOGGER_TAG, 0, "rabbitmq target ready")
								}
								countdown.CountDown()
							})
							if logf != nil {
								logf(klog.InfoLevel, DEFAULT_LOGGER_TAG, 0, "start rabbitmq target, [%s], route: %s", strings.Join(rabbitConfig.Addrs, ", "), rabbitConfig.Producer.Router)
							}
							gMqTargetManager[item.MQType] = rabbitMq
							go func(rabbitMq *target.RabbitMQ) {
								err := rabbitMq.Start()
								if err != nil && logf != nil {
									logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "start rabbit mq target failed, %s", err.Error())
								}
							}(rabbitMq)
						}
					} else {
						if logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "rabbit target config is invalid")
						}
					}
				}

			case "redismq":
				{
					if redisConfig, ok := item.Item.(*mqConf.RedisConfig); ok {
						redisMq, err := target.NewRedisMQ(ctx, "RedisTarget", redisConfig, sendQueueSize, false, logf)
						if err != nil && logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "create redis mq target failed, %s", err.Error())
						} else {
							redisMq.SetOnReady(func(ready bool) {
								if logf != nil {
									logf(klog.InfoLevel, DEFAULT_LOGGER_TAG, 0, "redismq target ready")
								}
								countdown.CountDown()
							})
							if logf != nil {
								logf(klog.InfoLevel, DEFAULT_LOGGER_TAG, 0, "start redismq target, [%s], topic: [%s]", strings.Join(redisConfig.Addrs, ","), strings.Join(redisConfig.Topics, ","))
							}
							gMqTargetManager[item.MQType] = redisMq
							go func(redisMq *target.RedisMQ) {
								err := redisMq.Start()
								if err != nil && logf != nil {
									logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "start redis mq target failed, %s", err.Error())
								}
							}(redisMq)
						}
					} else {
						if logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "redis target config is invalid")
						}
					}
				}

			case "rocketmq":
				{
					if rocketConfig, ok := item.Item.(*mqConf.RocketConfig); ok {
						rocketMq, err := target.NewRocketMQ(ctx, "RocketTarget", rocketConfig, sendQueueSize, false, logf)
						if err != nil && logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "create rocket mq target failed, %s", err.Error())
						} else {
							rocketMq.SetOnReady(func(ready bool) {
								if logf != nil {
									logf(klog.InfoLevel, DEFAULT_LOGGER_TAG, 0, "rocketmq target ready")
								}
								countdown.CountDown()
							})
							if logf != nil {
								logf(klog.InfoLevel, DEFAULT_LOGGER_TAG, 0, "start rocketmq target, [%s], topic: [%s]", strings.Join(rocketConfig.Servers, ","), strings.Join(rocketConfig.Producer.Topics, ","))
							}
							gMqTargetManager[item.MQType] = rocketMq
							go func(rocketMq *target.RocketMQ) {
								err := rocketMq.Start()
								if err != nil && logf != nil {
									logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "start rocket mq target failed, %s", err.Error())
								}
							}(rocketMq)
						}
					} else {
						if logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "rocket target config is invalid")
						}
					}
				}

			case "mqtt3":
				{
					if mqttConfig, ok := item.Item.(*mqConf.MqttConfig); ok {
						mqttClient, err := target.NewMqttMQ(ctx, "MqttSource", mqttConfig, sendQueueSize, false, logf)
						if err != nil && logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "create mqtt3 mq target failed, %s", err.Error())
						} else {
							mqttClient.SetOnReady(func(ready bool) {
								if logf != nil {
									logf(klog.InfoLevel, DEFAULT_LOGGER_TAG, 0, "mqtt3 target ready")
								}
								countdown.CountDown()
							})
							if logf != nil {
								logf(klog.InfoLevel, DEFAULT_LOGGER_TAG, 0, "start mqtt3 target, %s, topic: [%s]", mqttConfig.Broker, strings.Join(mqttConfig.Topics, ","))
							}
							gMqTargetManager[item.MQType] = mqttClient
							go func(mqtt *target.MqttMQ) {
								err := mqtt.Start()
								if err != nil && logf != nil {
									logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "start mqtt3 mq target failed, %s", err.Error())
								}
							}(mqttClient)
						}
					} else {
						if logf != nil {
							logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "mqtt3 target config is invalid")
						}
					}
				}

			default:
				if logf != nil {
					logf(klog.ErrorLevel, DEFAULT_LOGGER_TAG, 0, "unknown mq target type: %s", item.MQType)
				}
			}
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

func loadOffsetCache(conf *config.Configure) *offset.OffsetSync {
	offsetSync := offset.NewOffsetSync(conf.SyncTime, conf.SyncFile, LogFunc)
	// 载入本地缓存文件
	if filesystem.IsFileExists(conf.SyncFile) {

		if buf, err := os.ReadFile(conf.SyncFile); err != nil {
			glog.Error("read file %s error: %v", conf.SyncFile, err)
		} else {
			tmap := map[string]map[string]map[string]int64{}
			if err = json.Unmarshal(buf, &tmap); err != nil {
				glog.Error("unmarshal file %s error: %v", conf.SyncFile, err)
			} else {
				catcheOffset := offsetSync.Records
				for mqType, topicMap := range tmap {
					catcheOffset[mqType] = map[string]map[string]int64{}
					for topic, partitionOffset := range topicMap {
						catcheOffset[mqType][topic] = map[string]int64{}
						for part, offset := range partitionOffset {
							glog.Info("load offset: mqType:%s topic:%s partition%d: offset:%d", mqType, topic, part, offset)
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

// 解析重放消息文件
func getReplayData(toHex bool, path string) []*router.GenericMessage {
	messages := make([]*router.GenericMessage, 0, 2048)

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
	reader := bufio.NewReaderSize(file, 512*1024) // 缓冲区

	if length < 1 {
		glog.Error("parse test.message error, document is empty")
		return nil
	}

	var firstErr error = nil
	var header string = ""
	recordCount := int64(0)
	lineNum := 0
	auto_exit := false

	for {
		firstErr = nil
		// line, err := reader.ReadString('\n')
		lineBytes, err := reader.ReadSlice('\n')
		lineNum++

		if nil != err && len(lineBytes) == 0 {
			// firstErr = err
			break
		}

		lineBytes = bytes.TrimSpace(lineBytes)
		//忽略行注释 与 空行
		if len(lineBytes) == 0 || lineBytes[0] == '#' || (lineBytes[0] == '/' && len(lineBytes) > 1 && lineBytes[1] == '/') {
			continue
		}

		// 处理 header
		if bytes.HasPrefix(lineBytes, []byte("test.message")) {
			// 一个document只允许一个 header
			if header == "" {
				header = string(lineBytes) // header 只执行一次
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
		topic := ""
		idx := bytes.IndexByte(lineBytes, ' ')
		if idx > -1 {
			topic = string(lineBytes[:idx]) // topic 通常很短，影响不大
			secField := bytes.TrimSpace(lineBytes[idx+1:])
			// 处理注释（# 或 //）
			if commentIdx := bytes.LastIndexByte(secField, '#'); commentIdx != -1 {
				secField = bytes.TrimSpace(secField[:commentIdx])
			} else if commentIdx := bytes.LastIndex(secField, []byte("//")); commentIdx != -1 {
				secField = bytes.TrimSpace(secField[:commentIdx])
			}

			// 去掉引号
			secField = bytes.Trim(secField, `'"`)

			var finalData []byte
			if toHex {
				if decoded, err := hex.DecodeString(string(secField)); err == nil {
					finalData = decoded
				} else {
					glog.Error("hex decode line: %d error: %s", lineNum, err.Error())
					continue
				}
			} else {
				s := string(secField)
				// 只有包含转义字符时才执行替换
				if strings.Contains(s, `\"`) || strings.Contains(s, `\\u`) {
					s = strings.ReplaceAll(s, `\"`, `"`)
					s = strings.ReplaceAll(s, `\\u`, `\u`)
				}
				finalData = []byte(s)
			}
			messages = append(messages, &router.GenericMessage{Topic: topic, Message: finalData})
			recordCount += int64(bytes.Count(secField, []byte{','}) + 1)
		} else {
			lineStr := string(lineBytes)
			if lineStr == "quit" || lineStr == "exit" || lineStr == "stop" || lineStr == "QUIT" || lineStr == "EXIT" || lineStr == "STOP" {
				messages = append(messages, &router.GenericMessage{Topic: "quit", Message: []byte("")})
				auto_exit = true
			} else {
				glog.Error("Error data: %s", lineStr)
				continue
			}
		}
	}

	if nil != firstErr {
		glog.Error("Error: %s", firstErr.Error())
		return nil
	}

	if len(messages) > 0 {
		if auto_exit {
			glog.Info("parse %s success, message count: %d, records: %d", path, len(messages)-1, recordCount)
		} else {
			glog.Info("parse %s success, message count: %d, records: %d", path, len(messages), recordCount)
		}
	}

	return messages
}

func generalMessage(handler *router.DispatchService, resetTimestamp bool, message *router.GenericMessage) {
	if len(message.Message) == 0 || len(message.Topic) == 0 {
		glog.Warrn("Message or topic is empty") // 修正拼写
		return
	}
	content := *(*string)(unsafe.Pointer(&message.Message))
	if resetTimestamp {
		// 不是JSON
		if !strings.HasPrefix(content, "{") && !strings.HasPrefix(content, "[") {

			// 使用 Builder 减少内存分配
			var sb strings.Builder
			sb.Grow(len(content) + 128) // 预分配容量，减少 realloc

			records := strings.Split(content, ",") // 一条消息中包含多条记录, 每条记录都需要重置时间戳
			for i, record := range records {
				if i > 0 {
					sb.WriteByte(',')
				}
				record = kstrings.TrimSpace(record)
				if record == "" {
					continue
				}
				tmpArr := strings.Split(record, "@")
				if len(tmpArr) != 2 {
					continue
				}

				pointName := tmpArr[0]
				originData := tmpArr[1]
				tmpArr2 := strings.Split(originData, ":")
				if len(tmpArr2) < 3 {
					continue // 不符合格式也丢弃
				}

				// 重置时间戳
				timestamp, _ := strconv.ParseInt(tmpArr2[2], 10, 64)
				newTimestamp := time.Now().Unix()
				if timestamp > 9999999999 {
					newTimestamp = time.Now().UnixMilli()
				}
				tmpArr2[2] = fmt.Sprintf("%d", newTimestamp)

				// 手动拼接
				sb.WriteString(pointName)
				sb.WriteByte('@')
				for j, part := range tmpArr2 {
					if j > 0 {
						sb.WriteByte(':')
					}
					sb.WriteString(part)
				}
			}

			content = sb.String()
		}
	}

	// 防止重置后 content 为空
	if len(content) == 0 {
		glog.Warrn("Message content is empty, origin content=%s", content)
		return
	}

	uuid, _ := kuuid.NewV1()
	uuidStr := uuid.ShortString()

	// 复用 GenericMessage 结构体（减少对象分配）
	sendMsg := &router.GenericMessage{
		Topic:      message.Topic,
		Properties: map[string]string{"key": uuidStr}, // 仍然每次创建，后面可进一步优化
	}
END_SEND:
	for {
		bytesPtr := unsafe.Slice(unsafe.StringData(content), len(content))
		sendMsg.Message = bytesPtr
		status, err := handler.DoSend(sendMsg)
		// 发送成功 or 源服务排水中 则直接退出发送
		if status || err == idl.ErrSrvDraining {
			break END_SEND
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
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
func onRecved(origin any, name string, topic string, partition int, offset int64, _ map[string]string, isCompress bool, toHex bool, manualAck bool, message []byte) {
	// 源数据是否为压缩数据, 压缩数据必须是zip压缩算法
	var str string
	if isCompress {
		buf, err := data.UnZip(message)
		if err != nil {
			glog.Error("onRecved: name=%s, topic=%s, partition=%d, offset=%d, uncompress error= %v", name, topic, partition, offset, err)
			return
		}
		str = *(*string)(unsafe.Pointer(&buf)) // string(buf)
	} else {
		str = *(*string)(unsafe.Pointer(&message)) //string(message)
	}

	//// TODO 解析从消息中获取需要下发的topic 和 payload, 下发给下游MQ处理
	// flag := publish(to, topic, []byte(str), nil)
	// if !flag {
	// 	glog.Error("onRecived: name=%s, topic=%s, partition=%d, offset=%d, publish error= %s", name, topic, partition, offset, "sent to target faulted")
	// }

	var err error
	if !manualAck {
		err = messageAck(origin)
	}

	if err == nil {
		dataStr := str
		if toHex {
			slicePtr := unsafe.Slice(unsafe.StringData(str), len(str)) // 获取字符串底层切片, 只读方式的
			dataStr = hex.EncodeToString(slicePtr)
		}
		glog.Debug("onRecived: name=%s, topic=%s, partition=%d, offset=%d, message=%s", name, topic, partition, offset, dataStr)
		switch name {
		case "KafkaSource":
			if gOffsetSync != nil {
				gOffsetSync.Set("kafkamq", topic, strconv.Itoa(partition), offset)
			}
		case "RocketSource":
			if gOffsetSync != nil {
				gOffsetSync.Set("rocketmq", topic, strconv.Itoa(partition), offset)
			}
		case "NatsJSSource":
			if gOffsetSync != nil {
				gOffsetSync.Set("natsjsmq", topic, strconv.Itoa(partition), offset)
			}
		}
	} else {
		glog.Error("onRecved: name=%s, topic=%s, partition=%d, offset=%d, ack return error=%v", name, topic, partition, offset, err)
	}
}

///////////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////////////

func messageAck(origin any) error {
	var err error
	switch t := origin.(type) {
	case *rabbitmq.Message:
		err = t.Ack(false) // false: 只确认当前这条消息; true: 批量确认 DeliveryTag <= current DeliveryTag 的所有消息
	case *rocketmq.Message:
		err = t.Ack() // 批量确认
	case *kafkamq.KafkaMessage:
		err = t.Ack() // 确认当前消息seq之前的所有消息
	case nil:
		// 不支持ack的MQ 直接忽略
	default:
		// 其他
	}
	return err
}
