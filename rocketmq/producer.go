package rocketmq

import (
	"context"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/khan-lau/kutils/logger"
)

type RocketMessage struct {
	Topic      string
	Message    []byte
	Properties map[string]string
}

type Producer struct {
	ctx        context.Context
	cancel     context.CancelFunc
	mqProducer rocketmq.Producer
	queue      chan *RocketMessage // 消息队列
	conf       *RocketConfig
	logf       logger.AppLogFuncWithTag
}

func NewProducer(ctx context.Context, conf *RocketConfig, logf logger.AppLogFuncWithTag) (*Producer, error) {
	opts := make([]producer.Option, 0, 40)
	if conf.GroupName != "" {
		groupOption := producer.WithGroupName(conf.GroupName)
		opts = append(opts, groupOption)
	}

	if conf.Namespace != "" {
		namespaceOption := producer.WithNamespace(conf.Namespace)
		opts = append(opts, namespaceOption)
	}

	if conf.ClientID != "" {
		clientIdOption := producer.WithInstanceName(conf.ClientID)
		opts = append(opts, clientIdOption)
	}

	var serverOption producer.Option
	if conf.NsResolver {
		serverOption = producer.WithNsResolver(primitive.NewPassthroughResolver(conf.Servers))
	} else {
		namesrv, err := primitive.NewNamesrvAddr(conf.Servers...)
		if err != nil {
			return nil, err
		}

		serverOption = producer.WithNameServer(namesrv)
	}
	opts = append(opts, serverOption)

	if conf.Credentials != nil && conf.Credentials.AccessKey != "" {
		credentialsOption := producer.WithCredentials(primitive.Credentials{
			AccessKey: conf.Credentials.AccessKey,
			SecretKey: conf.Credentials.SecretKey,
		})
		opts = append(opts, credentialsOption)
	}

	retryOption := producer.WithRetry(conf.Producer.Retry)
	timeoutOption := producer.WithSendMsgTimeout(time.Duration(conf.Producer.Timeout) * time.Millisecond)
	queueOption := producer.WithQueueSelector(conf.Producer.QueueSelector)
	interceptorOption := producer.WithInterceptor(conf.Consumer.Interceptors...)

	opts = append(opts, retryOption, timeoutOption, queueOption, interceptorOption)

	rocketProducer, err := rocketmq.NewProducer(opts...)
	if err != nil {
		return nil, err
	}

	tProducer := &Producer{
		ctx:        ctx,
		mqProducer: rocketProducer,
		queue:      make(chan *RocketMessage, 1000),
		conf:       conf,
		logf:       logf,
	}

	subCtx, SubCancel := context.WithCancel(ctx)
	tProducer.ctx, tProducer.cancel = subCtx, SubCancel

	return tProducer, nil
}

func (that *Producer) Start() {
	subCtx := context.WithoutCancel(that.ctx)

	go func(mqProducer rocketmq.Producer) {
		err := mqProducer.Start()
		if err != nil && that.logf != nil {
			that.logf(logger.ErrorLevel, rocket_tag, "Start producer error: {}", err.Error())
		} else if that.logf != nil {
			that.logf(logger.InfoLevel, rocket_tag, "Start producer successfully")
		}
	}(that.mqProducer)

END_LOOP:
	for {
		select {
		case <-that.ctx.Done():
			break END_LOOP
		case msg := <-that.queue:
			mqMessage := primitive.NewMessage(msg.Topic, msg.Message)
			if msg.Properties != nil {
				mqMessage.WithProperties(msg.Properties)
			}
			if that.conf.Producer.AsyncSend {
				_ = that.mqProducer.SendAsync(subCtx, func(ctx context.Context, result *primitive.SendResult, err error) {
					if err != nil {
						if that.logf != nil {
							that.logf(logger.ErrorLevel, rocket_tag, "Send message error: {}", err.Error())
						}

						if that.conf.OnError != nil {
							that.conf.OnError(err)
						}
					}
				}, mqMessage)
			} else {
				_, err := that.mqProducer.SendSync(subCtx, mqMessage)
				if err != nil {
					if that.logf != nil {
						that.logf(logger.ErrorLevel, rocket_tag, "Send message error: {}", err.Error())
					}
					if that.conf.OnError != nil {
						that.conf.OnError(err)
					}
				}
			}
		}
	}

	if that.conf.OnExit != nil {
		that.conf.OnExit(nil)
	}
}

func (that *Producer) Publish(msg *RocketMessage) {
	that.queue <- msg
}

func (that *Producer) PublishMessage(topic string, message []byte) {
	msg := &RocketMessage{
		Topic:   topic,
		Message: message,
	}
	that.Publish(msg)
}

func (that *Producer) PublishData(topic string, message []byte, properties map[string]string) {
	msg := &RocketMessage{
		Topic:      topic,
		Message:    message,
		Properties: properties,
	}
	that.Publish(msg)
}

func (that *Producer) Close() {
	that.cancel()
	err := that.mqProducer.Shutdown()
	if err != nil && that.logf != nil {
		that.logf(logger.ErrorLevel, rocket_tag, "Shutdown producer error: {}", err.Error())
	}
}
