package rocketmq

import (
	"time"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

const (
	rocket_tag = "rocketmq"
)

/////////////////////////////////////////////////////////////

type ErrorCallbackFunc func(err error)
type EventCallbackFunc func(event interface{})

/////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////

type Message struct {
	*primitive.MessageExt
	consumer *PullConsumer
}

// 批量确认
func (that *Message) Ack() error {
	queue := that.Queue
	if queue != nil && that.consumer != nil {
		// that.consumer.mqConsumer.Search
		if err := that.consumer.mqConsumer.UpdateOffset(queue, that.QueueOffset); err != nil {
			return err
		} else {
			if err := that.consumer.mqConsumer.PersistOffset(that.consumer.ctx.Context(), that.Topic); err != nil {
				return err
			}
		}
	}
	return nil
}

/////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////

type RocketConsumerConfig struct {
	Topics              []string
	Mode                consumer.MessageModel     // BroadCasting 广播模式;  Clustering 集群模式; 默认为 Clustering
	Offset              consumer.ConsumeFromWhere // ConsumeFromFirstOffset 最新消息;  ConsumeFromLastOffset  最旧消息; ConsumeFromTimestamp 指定时间戳开始消费
	Timestamp           string                    // 指定时间戳开始消费, "20131223171201"
	Order               bool                      // 是否顺序消费, 默认为 false
	AutoCommit          bool                      // 自动确认, 默认为 true
	MessageBatchMaxSize int                       // 批量消费消息的最大数量, 默认为 1
	MaxReconsumeTimes   int                       // 最大重消费次数, 默认为 -1
	Interceptors        []primitive.Interceptor   // 消息拦截器, 默认为空
}

func NewRocketConsumerConfig() *RocketConsumerConfig {
	return &RocketConsumerConfig{
		Topics:       []string{},
		Interceptors: []primitive.Interceptor{},
	}
}

func (that *RocketConsumerConfig) SetTopics(topics ...string) *RocketConsumerConfig {
	that.Topics = topics
	return that
}

func (that *RocketConsumerConfig) AddTopic(topic string) *RocketConsumerConfig {
	that.Topics = append(that.Topics, topic)
	return that
}

func (that *RocketConsumerConfig) RemoveTopic(topic string) *RocketConsumerConfig {
	for i, v := range that.Topics {
		if v == topic {
			that.Topics = append(that.Topics[:i], that.Topics[i+1:]...)
		}
	}
	return that
}

func (that *RocketConsumerConfig) SetMode(mode consumer.MessageModel) *RocketConsumerConfig {
	that.Mode = mode
	return that
}

func (that *RocketConsumerConfig) SetOffset(offset consumer.ConsumeFromWhere) *RocketConsumerConfig {
	that.Offset = offset
	return that
}

func (that *RocketConsumerConfig) SetTimestamp(timestamp string) *RocketConsumerConfig {
	that.Timestamp = timestamp
	return that
}

func (that *RocketConsumerConfig) SetOrder(order bool) *RocketConsumerConfig {
	that.Order = order
	return that
}

func (that *RocketConsumerConfig) SetAutoCommit(autoCommit bool) *RocketConsumerConfig {
	that.AutoCommit = autoCommit
	return that
}

func (that *RocketConsumerConfig) SetMessageBatchMaxSize(messageBatchMaxSize int) *RocketConsumerConfig {
	that.MessageBatchMaxSize = messageBatchMaxSize
	return that
}

func (that *RocketConsumerConfig) SetMaxReconsumeTimes(maxReconsumeTimes int) *RocketConsumerConfig {
	that.MaxReconsumeTimes = maxReconsumeTimes
	return that
}

func (that *RocketConsumerConfig) SetInterceptor(interceptors []primitive.Interceptor) *RocketConsumerConfig {
	that.Interceptors = interceptors
	return that
}

////////////////////////////////////////////////////

////////////////////////////////////////////////////

type RocketProducerConfig struct {
	Topics        []string
	Timeout       time.Duration           // 消息发送超时时间, 单位为毫秒
	Retry         int                     // 消息发送重试次数
	QueueSelector producer.QueueSelector  // 消息队列选择策略, NewRandomQueueSelector 随机选择队列; NewRoundRobinQueueSelector 按照轮训方式选择队列; NewManualQueueSelector 直接选择消息中配置的队列
	Interceptors  []primitive.Interceptor // 消息拦截器, 默认为空
	AsyncSend     bool                    // 是否异步发送消息, 默认为 false
	// Tls           bool                    // 是否使用 TLS
}

func NewRocketProducerConfig() *RocketProducerConfig {
	return &RocketProducerConfig{
		Topics:       []string{},
		Interceptors: []primitive.Interceptor{},
	}
}

func (that *RocketProducerConfig) SetTopics(topics ...string) *RocketProducerConfig {
	that.Topics = topics
	return that
}

func (that *RocketProducerConfig) AddTopic(topic string) *RocketProducerConfig {
	that.Topics = append(that.Topics, topic)
	return that
}

func (that *RocketProducerConfig) RemoveTopic(topic string) *RocketProducerConfig {
	for i, v := range that.Topics {
		if v == topic {
			that.Topics = append(that.Topics[:i], that.Topics[i+1:]...)
		}
	}
	return that
}

func (that *RocketProducerConfig) SetTimeout(timeout time.Duration) *RocketProducerConfig {
	that.Timeout = timeout
	return that
}

func (that *RocketProducerConfig) SetRetry(retry int) *RocketProducerConfig {
	that.Retry = retry
	return that
}

func (that *RocketProducerConfig) SetQueueSelector(queueSelector producer.QueueSelector) *RocketProducerConfig {
	that.QueueSelector = queueSelector
	return that
}

func (that *RocketProducerConfig) SetInterceptor(interceptors []primitive.Interceptor) *RocketProducerConfig {
	that.Interceptors = interceptors
	return that
}

func (that *RocketProducerConfig) SetAsyncSend(async bool) *RocketProducerConfig {
	that.AsyncSend = async
	return that
}

// func (that *RocketProducerConfig) SetTls(tls bool) *RocketProducerConfig {
// 	that.Tls = tls
// 	return that
// }

////////////////////////////////////////////////////

////////////////////////////////////////////////////

type RocketConfig struct {
	NsResolver  bool     // 路由 或 域名解析
	Servers     []string // 服务列表
	GroupName   string
	ClientID    string
	Namespace   string                 // 命名空间
	Credentials *primitive.Credentials // 鉴权信息

	Consumer *RocketConsumerConfig
	Producer *RocketProducerConfig

	OnError ErrorCallbackFunc // 设置错误回调
	OnExit  EventCallbackFunc // 设置退出回调
}

func NewRocketConfig() *RocketConfig {
	return &RocketConfig{
		Servers: []string{},
	}
}

func (that *RocketConfig) SetNsResolver(nsResolver bool) *RocketConfig {
	that.NsResolver = nsResolver
	return that
}

func (that *RocketConfig) SetServers(servers ...string) *RocketConfig {
	that.Servers = servers
	return that
}

func (that *RocketConfig) AddServer(server string) *RocketConfig {
	that.Servers = append(that.Servers, server)
	return that
}

func (that *RocketConfig) RemoveServer(server string) *RocketConfig {
	for i, v := range that.Servers {
		if v == server {
			that.Servers = append(that.Servers[:i], that.Servers[i+1:]...)
		}
	}
	return that
}

func (that *RocketConfig) SetGroupName(groupName string) *RocketConfig {
	that.GroupName = groupName
	return that
}

func (that *RocketConfig) SetClientID(clientID string) *RocketConfig {
	that.ClientID = clientID
	return that
}

func (that *RocketConfig) SetNamespace(namespace string) *RocketConfig {
	that.Namespace = namespace
	return that
}

func (that *RocketConfig) SetCredentials(credentials *primitive.Credentials) *RocketConfig {
	that.Credentials = credentials
	return that
}

func (that *RocketConfig) SetCredentialsKey(accesskey string, secretkey string) *RocketConfig {
	that.Credentials = &primitive.Credentials{
		AccessKey: accesskey,
		SecretKey: secretkey,
	}
	return that
}

func (that *RocketConfig) SetConsumer(consumer *RocketConsumerConfig) *RocketConfig {
	that.Consumer = consumer
	return that
}

func (that *RocketConfig) SetProducer(producer *RocketProducerConfig) *RocketConfig {
	that.Producer = producer
	return that
}

func (that *RocketConfig) SetErrorCallback(callback ErrorCallbackFunc) *RocketConfig {
	that.OnError = callback
	return that
}

func (that *RocketConfig) SetExitCallback(callback EventCallbackFunc) *RocketConfig {
	that.OnExit = callback
	return that
}

////////////////////////////////////////////////////
