package rabbitmq

import (
	"github.com/khan-lau/kutils/logger"
)

type RabbitWorkType uint8

const (
	RABBIT_TYPE_SAMPLE = RabbitWorkType(iota)     // 简单模式, 一个消息生产者 一个消息消费者 一个队列 也称为点对点模式、一对一模式
	RABBIT_TYPE_WORK   = RabbitWorkType(iota + 1) // 工作模式, 一个生产者 多个消费者 每条消息只能被一个消费者消费 支持并发消费消息, 也是一对一消费模式
	RABBIT_TYPE_PUBSUB = RabbitWorkType(iota + 2) // 发布订阅模式, 一个生产者发送的消息会被多个消费者获取 因为一条消息会被多个消费者分别消费处理 所以也叫广播模式、一对多模式
	RABBIT_TYPE_ROUTER = RabbitWorkType(iota + 3) // 路由模式, 跟发布订阅模式相似, 区别在于发布订阅模式将消息转发给所有绑定的队列 而`路由模式`将消息转发给那个队列是`根据路由匹配情况`决定
	RABBIT_TYPE_TOPIC  = RabbitWorkType(iota + 4) // 主题模式, 跟路由模式类似 区别在于`主题模式`的路由匹配`支持通配符模糊匹配` 而`路由模式`仅支持`完全匹配`
	RABBIT_TYPE_RPC    = RabbitWorkType(iota + 5) // RPC模式
)

const (
	loggingPrefix = "RABBIT"
	rabbit_tag    = "rabbitmq"
)

/////////////////////////////////////////////////////////////

type ErrorCallbackFunc func(err error)
type EventCallbackFunc func(event interface{})

/////////////////////////////////////////////////////////////

type GoRabbitLogger struct {
	logf logger.AppLogFuncWithTag
}

// Fatalf -
func (that GoRabbitLogger) Fatalf(format string, v ...interface{}) {
	if that.logf != nil {
		that.logf(logger.FatalLevel, loggingPrefix, format, v...)
	}
}

// Errorf -
func (that GoRabbitLogger) Errorf(format string, v ...interface{}) {
	if that.logf != nil {
		that.logf(logger.ErrorLevel, loggingPrefix, format, v...)
	}
}

// Warnf -
func (that GoRabbitLogger) Warnf(format string, v ...interface{}) {
	if that.logf != nil {
		that.logf(logger.WarnLevel, loggingPrefix, format, v...)
	}
}

// Infof -
func (that GoRabbitLogger) Infof(format string, v ...interface{}) {
	if that.logf != nil {
		that.logf(logger.InfoLevel, loggingPrefix, format, v...)
	}
}

// Debugf -
func (that GoRabbitLogger) Debugf(format string, v ...interface{}) {
	if that.logf != nil {
		that.logf(logger.DebugLevel, loggingPrefix, format, v...)
	}
}

////////////////////////////////////////////////////

type ConsumerConfig struct {
	QueueName  string // 队列名称
	Exchange   string // 交换机名称
	KRouterKey string // 路由键
	WorkType   string // 工作模式
}

func NewConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{}
}

// func NewConsumerConfig(queueName, exchange, kRouterKey, workType string) *ConsumerConfig {
// 	return &ConsumerConfig{
// 		QueueName:  queueName,
// 		Exchange:   exchange,
// 		KRouterKey: kRouterKey,
// 		WorkType:   workType,
// 	}
// }

func (that *ConsumerConfig) SetQueueName(queueName string) *ConsumerConfig {
	that.QueueName = queueName
	return that
}

func (that *ConsumerConfig) SetExchange(exchange string) *ConsumerConfig {
	that.Exchange = exchange
	return that
}

func (that *ConsumerConfig) SetRouterKey(kRouterKey string) *ConsumerConfig {
	that.KRouterKey = kRouterKey
	return that
}

func (that *ConsumerConfig) SetWorkType(workType string) *ConsumerConfig {
	that.WorkType = workType
	return that
}

////////////////////////////////////////////////////

type ProducerConfig struct {
	Exchange string // 交换机名称
	Router   string // 路由键
	WorkType string // 工作模式
}

func NewProducerConfig() *ProducerConfig {
	return &ProducerConfig{}
}

// func NewProducerConfig(exchange, router, workType string) *ProducerConfig {
// 	return &ProducerConfig{
// 		Exchange: exchange,
// 		Router:   router,
// 		WorkType: workType,
// 	}
// }

func (that *ProducerConfig) SetExchange(exchange string) *ProducerConfig {
	that.Exchange = exchange
	return that
}

func (that *ProducerConfig) SetRouter(router string) *ProducerConfig {
	that.Router = router
	return that
}

func (that *ProducerConfig) SetWorkType(workType string) *ProducerConfig {
	that.WorkType = workType
	return that
}

////////////////////////////////////////////////////

type RabbitConfig struct {
	User     string // 用户名
	Password string // 密码
	Host     string // 主机地址
	Port     uint16 // 端口号
	VHost    string // 虚拟主机

	Consumer *ConsumerConfig // 设置消费配置
	Producer *ProducerConfig // 设置生产配置

	OnError ErrorCallbackFunc // 设置错误回调
	OnExit  EventCallbackFunc // 设置退出回调
}

func NewRabbitConfig() *RabbitConfig {
	return &RabbitConfig{}
}

func (that *RabbitConfig) SetUser(user string) *RabbitConfig {
	that.User = user
	return that
}

func (that *RabbitConfig) SetPassword(password string) *RabbitConfig {
	that.Password = password
	return that
}

func (that *RabbitConfig) SetHost(host string) *RabbitConfig {
	that.Host = host
	return that
}

func (that *RabbitConfig) SetPort(port uint16) *RabbitConfig {
	that.Port = port
	return that
}

func (that *RabbitConfig) SetVHost(vhost string) *RabbitConfig {
	that.VHost = vhost
	return that
}

func (that *RabbitConfig) SetConsumer(consumer *ConsumerConfig) *RabbitConfig {
	that.Consumer = consumer
	return that
}

func (that *RabbitConfig) SetProducer(producer *ProducerConfig) *RabbitConfig {
	that.Producer = producer
	return that
}

func (that *RabbitConfig) SetErrorCallback(callback ErrorCallbackFunc) *RabbitConfig {
	that.OnError = callback
	return that
}

func (that *RabbitConfig) SetExitCallback(callback EventCallbackFunc) *RabbitConfig {
	that.OnExit = callback
	return that
}
