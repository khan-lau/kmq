package rabbitmq

import (
	"github.com/khan-lau/kutils/container/kstrings"
	"github.com/khan-lau/kutils/logger"
	"github.com/wagslane/go-rabbitmq"
)

type SubscribeCallback func(voidObj interface{}, msg *rabbitmq.Delivery)

type Consumer struct {
	conn     *rabbitmq.Conn
	consumer *rabbitmq.Consumer
	// queue    chan<- []byte // 只读消息队列
	conf *RabbitConfig
	logf logger.AppLogFuncWithTag
}

func NewConsumer(conf *RabbitConfig, logf logger.AppLogFuncWithTag) (*Consumer, error) {
	logger := &GoRabbitLogger{logf: logf}

	conn, err := rabbitmq.NewConn(
		kstrings.FormatString("amqp://{}:{}@{}:{}{}", conf.User, conf.Password, conf.Host, int(conf.Port), conf.VHost),
		rabbitmq.WithConnectionOptionsLogger(logger),
	)

	if err != nil {
		return nil, err
	}

	consumer, err := rabbitmq.NewConsumer(
		conn,
		conf.Consumer.QueueName,
		// rabbitmq.WithConsumerOptionsConcurrency(2), // 并发数协程数量, go-rabbitmq内部参数
		// rabbitmq.WithConsumerOptionsConsumerName("consumer_1"),// 消费者名称, 不填写会自动生成
		rabbitmq.WithConsumerOptionsRoutingKey(conf.Consumer.KRouterKey), // 路由key, 可以多次设置
		rabbitmq.WithConsumerOptionsExchangeName(conf.Consumer.Exchange),
		rabbitmq.WithConsumerOptionsLogger(logger),
		rabbitmq.WithConsumerOptionsExchangeKind(conf.Consumer.WorkType), // direct, fanout, topic, headers
		rabbitmq.WithConsumerOptionsExchangeDurable,                      // Durable true
		// rabbitmq.WithConsumerOptionsExchangeAutoDelete,                   // AutoDelete true
		// rabbitmq.WithConsumerOptionsExchangeInternal,                     // Internal true
		rabbitmq.WithConsumerOptionsExchangeDeclare, // Declare true

		rabbitmq.WithConsumerOptionsQueueDurable, // Durable true
		rabbitmq.WithConsumerOptionsBinding(rabbitmq.Binding{
			RoutingKey: conf.Consumer.KRouterKey,
			BindingOptions: rabbitmq.BindingOptions{
				NoWait:  false,
				Args:    rabbitmq.Table{},
				Declare: true,
			}}),
	)

	if err != nil {
		conn.Close()
		return nil, err
	}

	return &Consumer{conn: conn, consumer: consumer, conf: conf, logf: logf}, nil
}

func (that *Consumer) Subscribe(voidObj interface{}, callback SubscribeCallback) {
	go that.SyncSubscribe(voidObj, callback)
}

func (that *Consumer) SyncSubscribe(voidObj interface{}, callback SubscribeCallback) {
	// block main thread - wait for shutdown signal
	err := that.consumer.Run(func(d rabbitmq.Delivery) rabbitmq.Action {
		// if that.logf != nil {
		// 	that.logf(logger.InfoLevel, "received: %s", string(d.Body))
		// }

		// that.queue <- d.Body
		if callback != nil {
			callback(voidObj, &d)
		}

		// rabbitmq.Ack, rabbitmq.NackDiscard, rabbitmq.NackRequeue
		return rabbitmq.Ack
	})

	if err != nil {
		if that.logf != nil {
			that.logf(logger.ErrorLevel, rabbit_tag, "consumer error: {}", err)
		}
	}
	if that.conf.OnExit != nil {
		that.conf.OnExit(nil)
	}
}

func (that *Consumer) Close() {
	if that.consumer != nil {
		that.consumer.Close()
	}
	if that.conn != nil {
		that.conn.Close()
	}
}
