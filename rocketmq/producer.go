package rocketmq

import (
	"context"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/khan-lau/kutils/container/kcontext"
	klog "github.com/khan-lau/kutils/klogger"
)

type RocketMessage struct {
	Topic      string
	Message    []byte
	Properties map[string]string
}

type Producer struct {
	ctx        *kcontext.ContextNode
	mqProducer rocketmq.Producer
	queue      chan *RocketMessage // 消息队列
	chanSize   uint                // 队列大小
	conf       *RocketConfig
	logf       klog.AppLogFuncWithTag
}

func NewProducer(ctx *kcontext.ContextNode, chanSize uint, conf *RocketConfig, logf klog.AppLogFuncWithTag) (*Producer, error) {
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
	timeoutOption := producer.WithSendMsgTimeout(time.Duration(conf.Producer.Timeout))
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
		queue:      make(chan *RocketMessage, chanSize),
		chanSize:   chanSize,
		conf:       conf,
		logf:       logf,
	}

	subCtx := ctx.NewChild("rocketmq_producer")
	tProducer.ctx = subCtx

	return tProducer, nil
}

func (that *Producer) Start() {
	go func(mqProducer rocketmq.Producer) {
		err := mqProducer.Start()
		if err != nil && that.logf != nil {
			that.logf(klog.ErrorLevel, rocket_tag, "Start producer error: {}", err.Error())
		} else if that.logf != nil {
			that.logf(klog.InfoLevel, rocket_tag, "Start producer successfully")
		}
	}(that.mqProducer)

END_LOOP:
	for {
		select {
		case <-that.ctx.Context().Done():
			break END_LOOP
		case msg := <-that.queue:
			mqMessage := primitive.NewMessage(msg.Topic, msg.Message)
			if msg.Properties != nil {
				mqMessage.WithProperties(msg.Properties)
			}
			if that.conf.Producer.AsyncSend {
				_ = that.mqProducer.SendAsync(that.ctx.Context(), func(ctx context.Context, result *primitive.SendResult, err error) {
					if err != nil {
						if that.logf != nil {
							that.logf(klog.ErrorLevel, rocket_tag, "Send message error: {}", err.Error())
						}

						if that.conf.OnError != nil {
							that.conf.OnError(err)
						}
					}
				}, mqMessage)
			} else {
				_, err := that.mqProducer.SendSync(that.ctx.Context(), mqMessage)
				if err != nil {
					if that.logf != nil {
						that.logf(klog.ErrorLevel, rocket_tag, "Send message error: {}", err.Error())
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

func (that *Producer) Publish(msg *RocketMessage) bool {
	if that != nil && that.queue != nil {
		select {
		case that.queue <- msg:
			return true
		default:
		}
	}
	return false
}

func (that *Producer) PublishMessage(topic string, message []byte) bool {
	msg := &RocketMessage{
		Topic:   topic,
		Message: message,
	}
	return that.Publish(msg)
}

func (that *Producer) PublishData(topic string, message []byte, properties map[string]string) bool {
	msg := &RocketMessage{
		Topic:      topic,
		Message:    message,
		Properties: properties,
	}
	return that.Publish(msg)
}

func (that *Producer) Close() {
	that.ctx.Cancel()
	err := that.mqProducer.Shutdown()
	if err != nil && that.logf != nil {
		that.logf(klog.ErrorLevel, rocket_tag, "Shutdown producer error: {}", err.Error())
	}
	that.ctx.Remove()
}
