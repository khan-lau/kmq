package rocketmq

import (
	"context"
	"time"

	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/container/kstrings"
	klog "github.com/khan-lau/kutils/klogger"
)

type SubscribeCallback func(voidObj interface{}, msg *Message)

type PushConsumer struct {
	ctx        *kcontext.ContextNode
	mqConsumer rocketmq.PushConsumer
	queue      chan *Message // 消息队列
	conf       *RocketConfig
	logf       klog.AppLogFuncWithTag
}

func NewPushConsumer(ctx *kcontext.ContextNode, conf *RocketConfig, logf klog.AppLogFuncWithTag) (*PushConsumer, error) {
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

	tConsumer := &PushConsumer{
		mqConsumer: pushConsumer,
		queue:      make(chan *Message, 1000),
		conf:       conf,
		logf:       logf,
	}

	for _, topic := range conf.Consumer.Topics {
		err = pushConsumer.Subscribe(
			topic,
			consumer.MessageSelector{},
			func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
				for _, msg := range msgs {
					tConsumer.queue <- &Message{MessageExt: msg, consumer: nil}
				}

				// consumer.ConsumeSuccess 消费完成
				// consumer.ConsumeRetryLater 稍后重试 消息在当前无法处理，需要稍后再次投递。Broker 会将这条消息放入重试队列，并根据重试策略（例如指数退避）在一段时间后重新投递给你或其他的消费者
				// consumer.Commit 与 RocketMQ 的事务消息（Transactional Message）有关。它们用于生产者（Producer）在发送半消息（Half Message）后，对事务的最终状态进行确认
				// consumer.Rollback 与 RocketMQ 的事务消息（Transactional Message）有关。它们用于生产者（Producer）在发送半消息（Half Message）后，对事务的最终状态进行回滚
				// consumer.SuspendCurrentQueueAMoment 暂停当前队列的消息消费，直到下一次定时器触发。这对于临时处理消息堆积或维护任务很有用
				// 如需要精确的手工ack, 不要使用pushConsumer, 应该使用相对原始的PullConsumer
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

	if conf.OnReady != nil {
		conf.OnReady(true)
	}
	return tConsumer, nil
}

func (that *PushConsumer) Subscribe(voidObj interface{}, callback SubscribeCallback) {
	go that.SyncSubscribe(voidObj, callback)
}

func (that *PushConsumer) SyncSubscribe(voidObj interface{}, callback SubscribeCallback) {
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

func (that *PushConsumer) Close() {
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

//////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////

type PullConsumer struct {
	ctx        *kcontext.ContextNode
	mqConsumer rocketmq.PullConsumer
	queue      chan *Message // 消息队列
	pullSize   uint          // 一次拉取的最大消息数量, 不得超过 0x0FFFFFFF 条
	conf       *RocketConfig
	logf       klog.AppLogFuncWithTag
}

func NewPullConsumer(ctx *kcontext.ContextNode, conf *RocketConfig, pullSize uint, logf klog.AppLogFuncWithTag) (*PullConsumer, error) {
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

	pullConsumer, err := rocketmq.NewPullConsumer(opts...)
	if err != nil {
		return nil, err
	}

	tConsumer := &PullConsumer{
		mqConsumer: pullConsumer,
		queue:      make(chan *Message, 1000),
		pullSize:   pullSize,
		conf:       conf,
		logf:       logf,
	}

	// 在主循环中拉取消息
	for _, topic := range conf.Consumer.Topics {
		err := pullConsumer.Subscribe(topic, consumer.MessageSelector{
			Type:       consumer.TAG,
			Expression: "*",
		})
		if err != nil {
			return nil, err
		}
		// }

		// // err = pullConsumer.Start()
		// // if err != nil {
		// // 	return nil, err
		// // }

		// for _, topic := range conf.Consumer.Topics {
		pullCtx := ctx.NewChild(kstrings.Sprintf("rocketmq_{}_runloop_consumer", topic))
		go func(ctx *kcontext.ContextNode) {
		END_LOOP:
			for {
				select {
				case <-pullCtx.Context().Done():
					break END_LOOP
				default:
				}

				msgs, err := tConsumer.pull(ctx, int(pullSize))
				if err != nil {
					if logf != nil {
						logf(klog.ErrorLevel, rocket_tag, "consumer pull error: {}", err.Error())
					}
				} else {
					for _, msg := range msgs {
						tConsumer.queue <- msg
					}

					// err = tConsumer.mqConsumer.PersistOffset(ctx.Context(), topic)
					// if err != nil {
					// 	logf(klog.ErrorLevel, rocket_tag, "consumer persist offset error: {}", err.Error())
					// }
				}

			}
		}(pullCtx)
	}
	subCtx := ctx.NewChild("rocketmq_consumer")
	tConsumer.ctx = subCtx

	if conf.OnReady != nil {
		conf.OnReady(true)
	}
	return tConsumer, nil
}

func (that *PullConsumer) pull(ctx *kcontext.ContextNode, maxSize int) ([]*Message, error) {
	subCtx := ctx.NewChild("rocketmq_pull_consumer")
	resp, err := that.mqConsumer.Pull(subCtx.Context(), maxSize)
	if err != nil {
		time.Sleep(500 * time.Millisecond)
		return nil, err
	}

	msgs := make([]*Message, 0, len(resp.GetMessages()))
	switch resp.Status {
	case primitive.PullFound:
		// var queue *primitive.MessageQueue
		if len(resp.GetMessages()) <= 0 {
			return []*Message{}, nil
		}
		for _, msg := range resp.GetMessageExts() {
			// queue = msg.Queue
			msgs = append(msgs, &Message{MessageExt: msg, consumer: that})
		}
		// // UpdateOffset更新本地内存中的offset, 还需要PersistOffset持久化到队列
		// err = that.mqConsumer.UpdateOffset(queue, resp.NextBeginOffset)
		// if err != nil {
		// 	return nil, err
		// }

	case primitive.PullNoNewMsg, primitive.PullNoMsgMatched:
		// 没有新数据, 或者没有匹配的消息
		time.Sleep(500 * time.Millisecond)
		// return nil, kstrings.Errorf("no pull message, next: {}", resp.NextBeginOffset)
		return []*Message{}, nil
	case primitive.PullBrokerTimeout:
		// 网络超时, 延迟重试
		time.Sleep(500 * time.Millisecond)
		// return nil, kstrings.Errorf("pull broker timeout, next: {}", resp.NextBeginOffset)
		return []*Message{}, nil
	case primitive.PullOffsetIllegal:
		// 拉取的offset不合法, 延迟重试
		// return nil, kstrings.Errorf("pull offset illegal, next: {}", resp.NextBeginOffset)
		return []*Message{}, nil
	default:
		return nil, kstrings.Errorf("pull error: {}", resp.Status)
	}

	return msgs, nil
}

func (that *PullConsumer) Subscribe(voidObj interface{}, callback SubscribeCallback) {
	go that.SyncSubscribe(voidObj, callback)
}

func (that *PullConsumer) SyncSubscribe(voidObj interface{}, callback SubscribeCallback) {
	consumerErrChan := make(chan error)
	go func(mqConsumer rocketmq.PullConsumer) {
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
				if that.conf.Consumer.AutoCommit {
					err := msg.Ack()
					if err != nil && that.logf != nil {
						that.logf(klog.ErrorLevel, rocket_tag, "Ack topic {} message error: {}", msg.Topic, err.Error())
					}
				}
			}
		}
	}

	if that.conf.OnExit != nil {
		that.conf.OnExit(nil)
	}

}

func (that *PullConsumer) Close() {
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
