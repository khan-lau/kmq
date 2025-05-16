package rabbitmq

import (
	"context"

	"github.com/khan-lau/kutils/container/kstrings"
	klog "github.com/khan-lau/kutils/klogger"
	"github.com/wagslane/go-rabbitmq"
)

type RabbitMessage struct {
	Exchange string   `json:"exchange"`
	Router   []string `json:"router"`
	Body     []byte   `json:"body"`
}

func NewRabbitMessage(exchange string, routers []string, body []byte) *RabbitMessage {
	return &RabbitMessage{Exchange: exchange, Router: routers, Body: body}
}

/////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////

type Producer struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
	conn       *rabbitmq.Conn
	publisher  *rabbitmq.Publisher

	// queue <-chan *mean.MeanMSG // 只读消息队列
	queue chan *RabbitMessage // 消息队列
	conf  *RabbitConfig
	logf  klog.AppLogFuncWithTag
}

func NewProducer(ctx context.Context, conf *RabbitConfig, logf klog.AppLogFuncWithTag) (*Producer, error) {
	queue := make(chan *RabbitMessage, 1000)
	rlog := &GoRabbitLogger{logf: logf}
	conn, err := rabbitmq.NewConn(
		kstrings.FormatString("amqp://{}:{}@{}:{}{}", conf.User, conf.Password, conf.Host, int(conf.Port), conf.VHost),
		rabbitmq.WithConnectionOptionsLogger(rlog),
	)
	if err != nil {
		return nil, err
	}

	publisher, err := rabbitmq.NewPublisher(
		conn,
		rabbitmq.WithPublisherOptionsLogger(rlog),
		rabbitmq.WithPublisherOptionsExchangeName(conf.Producer.Exchange),
		rabbitmq.WithPublisherOptionsExchangeKind(conf.Producer.WorkType),
		rabbitmq.WithPublisherOptionsExchangeDurable,

		rabbitmq.WithPublisherOptionsExchangeDeclare,
	)
	if err != nil {
		return nil, err
	}

	publisher.NotifyReturn(func(r rabbitmq.Return) {
		if logf != nil {
			logf(klog.DebugLevel, rabbit_tag, "message returned from server: {}", string(r.Body))
		}
	})

	publisher.NotifyPublish(func(c rabbitmq.Confirmation) {
		if logf != nil {
			logf(klog.DebugLevel, rabbit_tag, "message confirmed from server. tag: {}, ack: {}", c.DeliveryTag, c.Ack)
		}
	})

	subCtx, subCancel := context.WithCancel(ctx)
	return &Producer{ctx: subCtx, cancelFunc: subCancel, conn: conn, publisher: publisher, queue: queue, conf: conf, logf: logf}, nil
}

func (that *Producer) Start() {
END_LOOP:
	for {
		select {
		case <-that.ctx.Done():
			break END_LOOP
		case msg := <-that.queue:
			err := that.publisher.PublishWithContext( //nolint
				that.ctx,
				msg.Body,
				msg.Router,
				rabbitmq.WithPublishOptionsExchange(msg.Exchange),
			)
			if err != nil {
				if that.logf != nil {
					that.logf(klog.ErrorLevel, rabbit_tag, "publish message error: {}", err)
				}

				if that.conf.OnError != nil {
					that.conf.OnError(err)
				}
			}
		}
	}

	if that.conf.OnExit != nil {
		that.conf.OnExit(nil)
	}
}

func (that *Producer) Publish(msg *RabbitMessage) bool {
	if that != nil && that.queue != nil {
		select {
		case that.queue <- msg:
			return true
		default:
		}
	}
	return false
}

func (that *Producer) PublishMessage(exchange string, router string, message string) bool {
	msg := &RabbitMessage{
		Exchange: exchange,
		Router:   []string{router},
		Body:     []byte(message),
	}
	return that.Publish(msg)
}

func (that *Producer) Close() {
	// if that.logf != nil {
	// 	that.logf(klog.InfoLevel, "close producer, %v", string(debug.Stack()))
	// }
	that.cancelFunc()

	if that.publisher != nil {
		that.publisher.Close()
	}

	if that.conn != nil {
		_ = that.conn.Close()
	}

}
