package rocketmq

import (
	"context"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/khan-lau/kutils/container/kcontext"
	klog "github.com/khan-lau/kutils/klogger"
)

type SubscribeCallback func(voidObj interface{}, msg *primitive.MessageExt)

type Consumer struct {
	ctx        *kcontext.ContextNode
	mqConsumer rocketmq.PushConsumer
	queue      chan *primitive.MessageExt // 消息队列
	conf       *RocketConfig
	logf       klog.AppLogFuncWithTag
}

func NewConsumer(ctx *kcontext.ContextNode, conf *RocketConfig, logf klog.AppLogFuncWithTag) (*Consumer, error) {
	opts := make([]consumer.Option, 0, 40)

	if conf.GroupName != "" {
		groupOption := consumer.WithGroupName(conf.GroupName)
		opts = append(opts, groupOption)
	}

	if conf.Namespace != "" {
		namespaceOption := consumer.WithNamespace(conf.Namespace)
		opts = append(opts, namespaceOption)
	}

	if conf.ClientID != "" {
		clientIdOption := consumer.WithInstance(conf.ClientID)
		opts = append(opts, clientIdOption)
	}

	var serverOption consumer.Option
	if conf.NsResolver {
		serverOption = consumer.WithNsResolver(primitive.NewPassthroughResolver(conf.Servers))
	} else {
		namesrv, err := primitive.NewNamesrvAddr(conf.Servers...)
		if err != nil {
			return nil, err
		}

		serverOption = consumer.WithNameServer(namesrv)
	}
	opts = append(opts, serverOption)

	if conf.Credentials.AccessKey != "" && conf.Credentials.SecretKey != "" {
		credentialsOption := consumer.WithCredentials(primitive.Credentials{
			AccessKey: conf.Credentials.AccessKey,
			SecretKey: conf.Credentials.SecretKey,
		})
		opts = append(opts, credentialsOption)
	}

	reConsumeOption := consumer.WithMaxReconsumeTimes(int32(conf.Consumer.MaxReconsumeTimes))
	batchMaxSizeOption := consumer.WithConsumeMessageBatchMaxSize(conf.Consumer.MessageBatchMaxSize)
	modeOption := consumer.WithConsumerModel(conf.Consumer.Mode)
	orderOption := consumer.WithConsumerOrder(conf.Consumer.Order)
	offsetOption := consumer.WithConsumeFromWhere(conf.Consumer.Offset)
	var timestampOption consumer.Option
	if conf.Consumer.Offset == consumer.ConsumeFromTimestamp {
		// timestampOption = consumer.WithConsumeTimestamp("20131223171201")
		timestampOption = consumer.WithConsumeTimestamp(conf.Consumer.Timestamp)
		opts = append(opts, timestampOption)
	}

	interceptorOption := consumer.WithInterceptor(conf.Consumer.Interceptors...)

	opts = append(opts,
		reConsumeOption, batchMaxSizeOption,
		modeOption, orderOption,
		offsetOption, interceptorOption)

	pushConsumer, err := rocketmq.NewPushConsumer(opts...)
	if err != nil {
		return nil, err
	}

	tConsumer := &Consumer{
		mqConsumer: pushConsumer,
		queue:      make(chan *primitive.MessageExt, 1000),
		conf:       conf,
		logf:       logf,
	}

	for _, topic := range conf.Consumer.Topics {
		err = pushConsumer.Subscribe(
			topic,
			consumer.MessageSelector{},
			func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
				for _, msg := range msgs {
					tConsumer.queue <- msg
				}
				return consumer.ConsumeSuccess, nil
			},
		)
		if err != nil {
			return nil, err
		}
	}
	// subCtx, SubCancel := context.WithCancel(ctx)
	subCtx := ctx.NewChild("rocketmq_consumer")

	tConsumer.ctx = subCtx

	return tConsumer, nil
}

func (that *Consumer) Subscribe(voidObj interface{}, callback SubscribeCallback) {
	go that.SyncSubscribe(voidObj, callback)
}

func (that *Consumer) SyncSubscribe(voidObj interface{}, callback SubscribeCallback) {
	consumerErrChan := make(chan error)
	go func(mqConsumer rocketmq.PushConsumer) {
		err := mqConsumer.Start()
		if err != nil {
			consumerErrChan <- err
			return
		}
	}(that.mqConsumer)

END_LOOP:
	for {
		select {
		case <-that.ctx.Context().Done():
			break END_LOOP
		case err := <-consumerErrChan:
			if that.logf != nil {
				that.logf(klog.ErrorLevel, rocket_tag, "Start consumer error: {}", err.Error())
			}
			break END_LOOP
		case msg := <-that.queue:
			if callback != nil {
				callback(voidObj, msg)
			}
		}
	}

	if that.conf.OnExit != nil {
		that.conf.OnExit(nil)
	}

}

func (that *Consumer) Close() {
	for _, topic := range that.conf.Consumer.Topics {
		_ = that.mqConsumer.Unsubscribe(topic)
	}
	close(that.queue)
	err := that.mqConsumer.Shutdown()
	if err != nil && that.logf != nil {
		that.logf(klog.ErrorLevel, rocket_tag, "Shutdown consumer error: {}", err.Error())
	}
	that.ctx.Cancel()
	that.ctx.Remove()
}
