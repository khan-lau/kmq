package nats

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"strings"
	"time"

	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/container/kstrings"
	"github.com/khan-lau/kutils/katomic"
	"github.com/nats-io/nats.go"

	klog "github.com/khan-lau/kutils/klogger"
)

//////////////////////////////////////////////////////////////

type NatsCoreClient struct {
	ctx  *kcontext.ContextNode
	conf *NatsClientConfig

	conn      *nats.Conn
	connected *katomic.Bool
	subs      map[string]*nats.Subscription // 订阅列表, key为主题名称, value为订阅对象
	queue     chan *NatsMessage             // 消息队列
	chanSize  uint                          // 队列大小

	logf klog.AppLogFuncWithTag
}

func NewNatsCoreClient(ctx *kcontext.ContextNode, chanSize uint, conf *NatsClientConfig, logf klog.AppLogFuncWithTag) (*NatsCoreClient, error) {
	if conf.CoreNats() == nil {
		return nil, kstrings.Errorf("NatsClientConfig.CoreNats is nil")
	}

	subCtx := ctx.NewChild("natsmq_pubsub")
	return &NatsCoreClient{
		ctx:       subCtx,
		conf:      conf,
		conn:      nil,
		connected: katomic.NewBool(false),
		subs:      make(map[string]*nats.Subscription),
		queue:     nil,
		chanSize:  chanSize,
		logf:      logf,
	}, nil
}

func (that *NatsCoreClient) Subscribe(voidObj interface{}, callback SubscribeCallback) {
	go that.SyncSubscribe(voidObj, callback)
}

func (that *NatsCoreClient) SyncSubscribe(voidObj interface{}, callback SubscribeCallback) {
	if that.connected.Load() { // 已连接, 不再重连
		if that.logf != nil {
			that.logf(klog.InfoLevel, natsmq_tag, "Client is connected, do nothing")
		}
		return
	}
	err := that.doConnect()
	if err != nil {
		if that.logf != nil {
			that.logf(klog.WarnLevel, natsmq_tag, "Connect error: {}", err)
		}
		return
	}

	// that.connected.Store(true)

	topics := that.conf.CoreNats().Topics()
	// 异步模式
	if len(that.conf.CoreNats().QueueGroup()) > 0 { // 非空时为负载均衡模式, core模式下 没有 类似kafka的通过key的hash实现的负载均衡模式
		// TODO 异步模式, 负载均衡模式
		for _, topic := range topics {
			sub, err := that.conn.QueueSubscribe(topic, that.conf.CoreNats().QueueGroup(), func(msg *nats.Msg) {
				// that.logf(klog.DebugLevel, natsmq_tag, "QueueSubscribe topic: {}, msg: {}", topic, string(msg.Data))
				if callback != nil {
					callback(voidObj, &NatsMessage{Topic: msg.Subject, Reply: msg.Reply, Header: msg.Header, Payload: msg.Data})
				}
			})
			if err != nil {
				if that.logf != nil {
					that.logf(klog.WarnLevel, natsmq_tag, "QueueSubscribe topic: {}, error: {}", topic, err)
				}
			} else {
				if err := sub.SetPendingLimits(that.conf.CoreNats().maxPending, -1); err == nil { // 设置消息队列大小限制, 字节数无限制
					that.subs[topic] = sub
				} else {
					if that.logf != nil {
						that.logf(klog.WarnLevel, natsmq_tag, "SetPendingLimits error: {}", err)
					}
				}
			}
		}
	} else {
		// TODO 异步模式, 非负载均衡模式
		for _, topic := range topics {
			sub, err := that.conn.Subscribe(topic, func(msg *nats.Msg) {
				// that.logf(klog.DebugLevel, natsmq_tag, "QueueSubscribe topic: {}, msg: {}", topic, string(msg.Data))
				if callback != nil {
					callback(voidObj, &NatsMessage{Topic: msg.Subject, Reply: msg.Reply, Header: msg.Header, Payload: msg.Data})
				}
			})
			if err != nil {
				if that.logf != nil {
					that.logf(klog.WarnLevel, natsmq_tag, "QueueSubscribe topic: {}, error: {}", topic, err)
				}
			} else {
				if err := sub.SetPendingLimits(that.conf.CoreNats().maxPending, -1); err == nil { // 设置消息队列大小限制, 字节数无限制
					that.subs[topic] = sub
				} else {
					if that.logf != nil {
						that.logf(klog.WarnLevel, natsmq_tag, "SetPendingLimits error: {}", err)
					}
				}
			}
		}
	}

	<-that.ctx.Context().Done()

	that.stop()
	if that.logf != nil {
		that.logf(klog.InfoLevel, natsmq_tag, "Client is done")
	}
	if that.conf.OnExit() != nil {
		that.conf.OnExit()(nil)
	}
}

func (that *NatsCoreClient) Start() {
	if that.connected.Load() { // 已连接, 不再重连
		if that.logf != nil {
			that.logf(klog.InfoLevel, natsmq_tag, "Client is connected, do nothing")
		}
		return
	}

	err := that.doConnect()
	if err != nil {
		if that.logf != nil {
			that.logf(klog.WarnLevel, natsmq_tag, "Connect error: {}", err)
		}
		return
	}

	that.queue = make(chan *NatsMessage, that.chanSize) //创建发送channel
	subCtx := that.ctx.NewChild("natsmq_pub")

END_LOOP:
	for {
		select {
		case <-that.ctx.Context().Done():
			{
				break END_LOOP
			}
		case msg := <-that.queue:
			{
				that.PublishData(msg)
			}
		}
	}

	subCtx.Cancel()
	subCtx.Remove()
	that.stop()
	if that.logf != nil {
		that.logf(klog.InfoLevel, natsmq_tag, "Client is done")
	}

	if that.conf.OnExit() != nil {
		that.conf.OnExit()(nil)
	}
}

func (that *NatsCoreClient) doConnect() error {
	opts := make([]nats.Option, 0, 30)
	opts = append(opts, nats.Name(that.conf.Nats().name)) // 设置客户端名称
	if len(that.conf.Nats().User()) > 0 && len(that.conf.Nats().Password()) > 0 {
		opts = append(opts, nats.UserInfo(that.conf.Nats().User(), that.conf.Nats().Password())) // 用户名密码模式
	} else if len(that.conf.Nats().User()) == 0 && len(that.conf.Nats().Password()) > 0 {
		opts = append(opts, nats.Token(that.conf.nats.Password())) // token模式
	}

	// 允许TLS连接
	if that.conf.Nats().UseTls() {
		// 加载客户端证书和密钥
		cert, err := tls.LoadX509KeyPair(that.conf.Nats().TlsClientCert(), that.conf.Nats().KeyPath())
		if err != nil {
			if that.logf != nil {
				that.logf(klog.WarnLevel, natsmq_tag, "Error parsing X509 certificate/key pair: {}", err)
			}
			return err
		}

		// 初始化 TLS 配置
		tlsConfig := &tls.Config{
			ServerName:   "", // 服务器名称，用于证书验证等用途, 此处无需设置, 由nats.go中内部自动设置, 具体见 `func (nc *Conn) makeTLSConn() error`
			Certificates: []tls.Certificate{cert},
			MinVersion:   uint16(that.conf.Nats().MinTlsVer()),
		}

		var systemCertPool *x509.CertPool
		if that.conf.Nats().insecureSkipVerify { // 允许无CA
			tlsConfig.InsecureSkipVerify = true
			tlsConfig.RootCAs = nil
		} else {
			// 获取证书池
			systemCertPool, err = x509.SystemCertPool()
			if err != nil {
				if that.logf != nil {
					that.logf(klog.WarnLevel, natsmq_tag, "Warning: Could not load system root CA pool: {}. Using empty pool.", err)
				}
				systemCertPool = x509.NewCertPool()
			}
			tlsConfig.RootCAs = systemCertPool
		}
		opts = append(opts, nats.Secure(tlsConfig))
	}

	if that.conf.Nats().AllowReconnect() {
		opts = append(opts, nats.MaxReconnects(that.conf.Nats().MaxReconnect()))
		opts = append(opts, nats.ReconnectWait(time.Duration(that.conf.Nats().ReconnectWait())*time.Millisecond))

		opts = append(opts, nats.ReconnectBufSize(that.conf.Nats().ReconnectBufSize())) // 在客户端与服务器连接断开时，临时缓存你发布的出站（outgoing）消息
	}

	opts = append(opts, nats.PingInterval(time.Duration(that.conf.Nats().PingInterval())*time.Millisecond)) // 设置ping间隔时间
	opts = append(opts, nats.MaxPingsOutstanding(that.conf.Nats().MaxPingsOut()))                           // 设置最大允许的ping无应答次数

	// 连接成功时调用
	opts = append(opts, nats.ConnectHandler(func(nc *nats.Conn) {
		that.connected.Store(true)
	}))

	// 连接断开时调用
	opts = append(opts, nats.DisconnectHandler(func(nc *nats.Conn) {
		that.connected.Store(false)
	}))

	// Connect to a server
	conn, err := nats.Connect(strings.Join(that.conf.nats.Servers(), ","), opts...) // 连接NATS服务器, 允许同时连接多个服务器地址, 逗号分隔
	if err != nil {
		if that.logf != nil {
			that.logf(klog.WarnLevel, natsmq_tag, "Connect error: {}", err)
		}
		if that.conf.OnError() != nil {
			that.conf.OnError()(err)
		}
		return err
	}

	that.conn = conn
	return nil
}

func (that *NatsCoreClient) stop() {
	for _, sub := range that.subs {
		if sub != nil {
			_ = sub.Drain() // 确保所有已投递但尚未处理的消息都能得到处理，这对于需要高可靠性、不能丢失任何消息的应用程序至关重要
			// sub.Unsubscribe() // 立即断开订阅，而不会等待任何队列中未处理的消息。这可能导致这些消息永远无法被你的程序处理
		}
	}
	if that.conn != nil {
		_ = that.conn.Drain() // 优雅地关闭连接，它会确保所有正在进行的订阅和发布操作都得到妥善处理, 阻塞操作
		// that.conn.Close() // 立即关闭客户端与 NATS 服务器的连接, 所有待发送的消息都会被刷新并发送给服务器, 不会等待任何订阅者处理完消息
	}
	// that.connected.Store(false)
	if that.queue != nil {
		close(that.queue)
	}
}

func (that *NatsCoreClient) Close() {
	that.ctx.Cancel()
	that.ctx.Remove()
}

func (that *NatsCoreClient) Publish(msg *NatsMessage) bool {
	if that != nil && that.queue != nil {
		select {
		case that.queue <- msg:
			return true
		default:
		}
	}
	return false
}

func (that *NatsCoreClient) PublishMessage(topic string, message string) bool {
	msg := &NatsMessage{
		Topic:   topic,
		Payload: []byte(message),
	}
	return that.Publish(msg)
}

func (that *NatsCoreClient) PublishData(msg *NatsMessage) {
	msgWithKey := nats.NewMsg(msg.Topic)
	msgWithKey.Reply = msg.Reply
	msgWithKey.Data = msg.Payload
	msgWithKey.Header = msg.Header

	// that.logf(klog.DebugLevel, natsmq_tag, "topic {}, PublishData: {}", msg.Topic, string(msg.Payload))

	err := that.conn.PublishMsg(msgWithKey)
	if err != nil {
		if that.logf != nil {
			that.logf(klog.WarnLevel, natsmq_tag, "Publish error: {}", err)
		}

		if that.conf.OnError() != nil {
			that.conf.OnError()(err)
		}
	}
}

//////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////

type NatsJetStreamClient struct {
	ctx       *kcontext.ContextNode
	conf      *NatsClientConfig
	conn      *nats.Conn
	connected *katomic.Bool
	timestamp int64                 // 上次消费的纳秒时间戳, 用于断点消费, -1 为无效值
	sub       *nats.Subscription    // 持有的订阅对象
	js        nats.JetStreamContext // JetStream 上下文
	stream    *nats.StreamInfo      // Stream 信息
	queue     chan *NatsMessage     // 消息队列
	chanSize  uint                  // 队列大小
	logf      klog.AppLogFuncWithTag
}

func NewNatsJetStreamClient(ctx *kcontext.ContextNode, chanSize uint, timestamp int64, conf *NatsClientConfig, logf klog.AppLogFuncWithTag) (*NatsJetStreamClient, error) {
	if conf.JetStream() == nil {
		return nil, kstrings.Errorf("NatsClientConfig.JetStream is nil")
	}

	subCtx := ctx.NewChild("natsmq_js_pubsub")
	return &NatsJetStreamClient{
		ctx:       subCtx,
		conf:      conf,
		conn:      nil,
		connected: katomic.NewBool(false),
		timestamp: timestamp, // 上次消费的纳秒时间戳, 用于断点消费, -1 为无效值
		sub:       nil,
		js:        nil,
		stream:    nil,
		queue:     nil,
		chanSize:  chanSize,
		logf:      logf,
	}, nil
}

func (that *NatsJetStreamClient) Start() {
	if that.connected.Load() { // 已连接, 不再重连
		if that.logf != nil {
			that.logf(klog.InfoLevel, natsmq_tag, "Client is connected, do nothing")
		}
		return
	}

	// 连接nats, 并创建jetstream
	err := that.doConnect()
	if err != nil {
		if that.logf != nil {
			that.logf(klog.WarnLevel, natsmq_tag, "Connect error: {}", err)
		}
		return
	}

	that.queue = make(chan *NatsMessage, that.chanSize) //创建发送channel
	subCtx := that.ctx.NewChild("natsmq_js_pub")

END_LOOP:
	for {
		select {
		case <-that.ctx.Context().Done():
			{
				break END_LOOP
			}
		case msg := <-that.queue:
			{
				that.PublishData(msg, "")
			}
		}
	}

	subCtx.Cancel()
	subCtx.Remove()
	that.stop()
	if that.logf != nil {
		that.logf(klog.InfoLevel, natsmq_tag, "Client is done")
	}

	if that.conf.OnExit() != nil {
		that.conf.OnExit()(nil)
	}
}

func (that *NatsJetStreamClient) Subscribe(voidObj interface{}, callback SubscribeCallback) {
	go that.SyncSubscribe(voidObj, callback)
}

func (that *NatsJetStreamClient) SyncSubscribe(voidObj interface{}, callback SubscribeCallback) {
	if that.connected.Load() { // 已连接, 不再重连
		if that.logf != nil {
			that.logf(klog.InfoLevel, natsmq_tag, "Client is connected, do nothing")
		}
		return
	}
	err := that.doConnect()
	if err != nil {
		if that.logf != nil {
			that.logf(klog.WarnLevel, natsmq_tag, "Connect error: {}", err)
		}
		return
	}

	// that.connected.Store(true)

	if that.logf != nil {
		that.logf(klog.DebugLevel, natsmq_tag, "Stream: {}", that.stream.Config)
	}

	consumer, err := that.upsertConsumer(that.js)
	if err != nil {
		if that.logf != nil {
			that.logf(klog.WarnLevel, natsmq_tag, "upsert Consumer error: {}", err)
		}
		return
	}

	if that.logf != nil {
		that.logf(klog.InfoLevel, natsmq_tag, "consumer: {}", consumer.Config)
	}

	if len(that.conf.JetStream().Consumer().Name()) > 0 {
		// 群组订阅
		sub, err := that.js.QueueSubscribe(">", that.conf.JetStream().Consumer().Name(), func(msg *nats.Msg) {
			// 1. 从消息中获取元数据
			meta, err := msg.Metadata()
			if err != nil {
				if that.logf != nil {
					that.logf(klog.WarnLevel, natsmq_tag, "Failed to get message metadata: {}", err)
				}
				return
			}
			if callback != nil {
				callback(voidObj, &NatsMessage{Seq: meta.Timestamp.UnixNano(), Topic: msg.Subject, Reply: msg.Reply, Header: msg.Header, Payload: msg.Data, origin: msg})
			}

			if that.conf.jetStream.consumer.AutoCommit() {
				err := msg.Ack()
				if err != nil && that.logf != nil {
					that.logf(klog.ErrorLevel, natsmq_tag, "Failed to ack topic: {}, message: {}", msg.Subject, err)
				}
			}
		}, nats.Durable(that.conf.JetStream().Consumer().Name()))

		if err != nil {
			if that.logf != nil {
				that.logf(klog.WarnLevel, natsmq_tag, "QueueSubscribe error: {}", err)
			}
			return
		}

		that.sub = sub
	} else {
		// 普通订阅
		sub, err := that.js.Subscribe(">", func(msg *nats.Msg) {
			// 1. 从消息中获取元数据
			meta, err := msg.Metadata()
			if err != nil {
				if that.logf != nil {
					that.logf(klog.WarnLevel, natsmq_tag, "Failed to get message metadata: {}", err)
				}
				return
			}
			if callback != nil {
				callback(voidObj, &NatsMessage{Seq: meta.Timestamp.UnixNano(), Topic: msg.Subject, Reply: msg.Reply, Header: msg.Header, Payload: msg.Data, origin: msg})
			}
			if err := msg.Ack(); err != nil {
				if that.logf != nil {
					that.logf(klog.ErrorLevel, natsmq_tag, "Failed to ack topic: {}, message: {}", msg.Subject, err)
				}
			}
		})
		if err != nil {
			if that.logf != nil {
				that.logf(klog.WarnLevel, natsmq_tag, "Subscribe error: {}", err)
			}
			return
		}
		that.sub = sub
	}

	<-that.ctx.Context().Done()

	that.stop()
	if that.logf != nil {
		that.logf(klog.InfoLevel, natsmq_tag, "Client is done")
	}
	if that.conf.OnExit() != nil {
		that.conf.OnExit()(nil)
	}
}

func (that *NatsJetStreamClient) upsertJetstream(js nats.JetStreamContext) (*nats.StreamInfo, error) {
	topics := that.conf.JetStream().Topics()

	jsCfg := &nats.StreamConfig{
		Name:              that.conf.JetStream().Name(),
		Subjects:          topics,
		Storage:           that.conf.JetStream().StorageType(),
		Compression:       that.conf.JetStream().Compression(),
		Retention:         that.conf.JetStream().RetentionPolicy(),
		MaxConsumers:      that.conf.JetStream().MaxConsumers(),
		MaxMsgs:           that.conf.JetStream().MaxMsgs(),
		MaxBytes:          that.conf.JetStream().MaxBytes(),
		MaxAge:            time.Duration(that.conf.JetStream().MaxAge()) * time.Millisecond,
		MaxMsgsPerSubject: that.conf.JetStream().MaxMsgsPerSubject(),
		MaxMsgSize:        that.conf.JetStream().MaxMsgSize(),
		Duplicates:        time.Duration(that.conf.JetStream().Duplicates()) * time.Millisecond,
		Discard:           that.conf.JetStream().Discard(),
	}

	// 创建jetstream
	stream, err := js.AddStream(jsCfg)
	if err != nil {
		if !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
			// if that.logf != nil {
			// 	that.logf(klog.WarnLevel, natsmq_tag, "AddStream error: {}", err)
			// }
			return nil, err
		} else {
			// 如果jetstream存在, 则更新配置
			stream, err = js.UpdateStream(jsCfg)
			if err != nil {
				if strings.Contains(err.Error(), "stream configuration update can not change MaxConsumers") {
					if that.logf != nil {
						that.logf(klog.WarnLevel, natsmq_tag, "Ignoring MaxConsumers update error: %v", err)
					}
				} else {
					// if that.logf != nil {
					// 	that.logf(klog.WarnLevel, natsmq_tag, "AddStream error: {}", err)
					// }
					// return nil, err
				}

			}
		}
	}
	return stream, nil
}

func (that *NatsJetStreamClient) upsertConsumer(js nats.JetStreamContext) (*nats.ConsumerInfo, error) {

	filterSubject := that.conf.JetStream().Consumer().FilterSubject()
	if len(filterSubject) == 0 { // 如果没有指定过滤主题, 则订阅所有主题, `>`为通配符
		filterSubject = ">"
	}
	consumerCfg := &nats.ConsumerConfig{
		FilterSubject: filterSubject,
		MaxWaiting:    int(that.conf.JetStream().Consumer().MaxWait()),
		AckPolicy:     that.conf.JetStream().Consumer().AckPolicy(),
		DeliverPolicy: that.conf.JetStream().Consumer().DeliverPolicy(),
	}

	if that.timestamp >= 0 {
		startTime := time.Unix(0, that.timestamp)
		consumerCfg.OptStartTime = &startTime // 断点续传
	} else {
		consumerCfg.OptStartTime = nil
	}

	consumer, err := js.AddConsumer(that.conf.JetStream().Name(), consumerCfg)
	if err != nil {
		if !errors.Is(err, nats.ErrConsumerNameAlreadyInUse) {
			// if that.logf != nil {
			// 	that.logf(klog.WarnLevel, natsmq_tag, "Consumer error: {}", err)
			// }
			return nil, err
		} else {
			consumer, err = js.UpdateConsumer(that.conf.JetStream().Consumer().Name(), consumerCfg)
			if err != nil {
				// if that.logf != nil {
				// 	that.logf(klog.WarnLevel, natsmq_tag, "UpdateConsumer error: {}", err)
				// }
				return nil, err
			}
		}
	}
	return consumer, nil
}

func (that *NatsJetStreamClient) doConnect() error {
	opts := make([]nats.Option, 0, 30)
	opts = append(opts, nats.Name(that.conf.Nats().name)) // 设置客户端名称
	if len(that.conf.Nats().User()) > 0 && len(that.conf.Nats().Password()) > 0 {
		opts = append(opts, nats.UserInfo(that.conf.Nats().User(), that.conf.Nats().Password())) // 用户名密码模式
	} else if len(that.conf.Nats().User()) == 0 && len(that.conf.Nats().Password()) > 0 {
		opts = append(opts, nats.Token(that.conf.nats.Password())) // token模式
	}

	// 允许TLS连接
	if that.conf.Nats().UseTls() {
		// 加载客户端证书和密钥
		cert, err := tls.LoadX509KeyPair(that.conf.Nats().TlsClientCert(), that.conf.Nats().KeyPath())
		if err != nil {
			if that.logf != nil {
				that.logf(klog.WarnLevel, natsmq_tag, "Error parsing X509 certificate/key pair: {}", err)
			}
			return err
		}

		// 初始化 TLS 配置
		tlsConfig := &tls.Config{
			ServerName:   "", // 服务器名称，用于证书验证等用途, 此处无需设置, 由nats.go中内部自动设置, 具体见 `func (nc *Conn) makeTLSConn() error`
			Certificates: []tls.Certificate{cert},
			MinVersion:   uint16(that.conf.Nats().MinTlsVer()),
		}

		var systemCertPool *x509.CertPool
		if that.conf.Nats().insecureSkipVerify { // 允许无CA
			tlsConfig.InsecureSkipVerify = true
			tlsConfig.RootCAs = nil
		} else {
			// 获取证书池
			systemCertPool, err = x509.SystemCertPool()
			if err != nil {
				if that.logf != nil {
					that.logf(klog.WarnLevel, natsmq_tag, "Warning: Could not load system root CA pool: {}. Using empty pool.", err)
				}
				systemCertPool = x509.NewCertPool()
			}
			tlsConfig.RootCAs = systemCertPool
		}
		opts = append(opts, nats.Secure(tlsConfig))
	}

	if that.conf.Nats().AllowReconnect() {
		opts = append(opts, nats.MaxReconnects(that.conf.Nats().MaxReconnect()))
		opts = append(opts, nats.ReconnectWait(time.Duration(that.conf.Nats().ReconnectWait())*time.Millisecond))

		opts = append(opts, nats.ReconnectBufSize(that.conf.Nats().ReconnectBufSize())) // 在客户端与服务器连接断开时，临时缓存你发布的出站（outgoing）消息
	}

	opts = append(opts, nats.PingInterval(time.Duration(that.conf.Nats().PingInterval())*time.Millisecond)) // 设置ping间隔时间
	opts = append(opts, nats.MaxPingsOutstanding(that.conf.Nats().MaxPingsOut()))                           // 设置最大允许的ping无应答次数

	// 连接成功时调用
	opts = append(opts, nats.ConnectHandler(func(nc *nats.Conn) {
		that.connected.Store(true)
	}))

	// 连接断开时调用
	opts = append(opts, nats.DisconnectHandler(func(nc *nats.Conn) {
		that.connected.Store(false)
	}))

	// Connect to a server
	conn, err := nats.Connect(strings.Join(that.conf.nats.Servers(), ","), opts...) // 连接NATS服务器, 允许同时连接多个服务器地址, 逗号分隔
	if err != nil {
		// if that.logf != nil {
		// 	that.logf(klog.WarnLevel, natsmq_tag, "Connect error: {}", err)
		// }
		if that.conf.OnError() != nil {
			that.conf.OnError()(err)
		}
		return err
	}

	that.conn = conn

	js, err := that.conn.JetStream()
	if err != nil {
		// if that.logf != nil {
		// 	that.logf(klog.WarnLevel, natsmq_tag, "JetStream error: {}", err)
		// }
		return err
	}
	that.js = js
	stream, err := that.upsertJetstream(js)
	if err != nil {
		// if that.logf != nil {
		// 	that.logf(klog.WarnLevel, natsmq_tag, "upsert JetStream error: {}", err)
		// }
		return err
	}

	that.stream = stream
	return nil
}

func (that *NatsJetStreamClient) stop() {
	if that.sub != nil {
		_ = that.sub.Drain() // 确保所有已投递但尚未处理的消息都能得到处理，这对于需要高可靠性、不能丢失任何消息的应用程序至关重要
		// sub.Unsubscribe() // 立即断开订阅，而不会等待任何队列中未处理的消息。这可能导致这些消息永远无法被你的程序处理
	}
	if that.conn != nil {
		_ = that.conn.Drain() // 优雅地关闭连接，它会确保所有正在进行的订阅和发布操作都得到妥善处理, 阻塞操作
		// that.conn.Close() // 立即关闭客户端与 NATS 服务器的连接, 所有待发送的消息都会被刷新并发送给服务器, 不会等待任何订阅者处理完消息
	}
	// that.connected.Store(false)
	if that.queue != nil {
		close(that.queue)
	}
}

func (that *NatsJetStreamClient) Close() {
	that.ctx.Cancel()
	that.ctx.Remove()
}

func (that *NatsJetStreamClient) Publish(msg *NatsMessage) bool {
	if that != nil && that.queue != nil {
		select {
		case that.queue <- msg:
			return true
		default:
		}
	}
	return false
}

func (that *NatsJetStreamClient) PublishMessage(topic string, message string) bool {
	msg := &NatsMessage{
		Topic:   topic,
		Payload: []byte(message),
	}
	return that.Publish(msg)
}

func (that *NatsJetStreamClient) PublishData(msg *NatsMessage, msgId string) {
	msgWithKey := nats.NewMsg(msg.Topic)
	msgWithKey.Reply = msg.Reply
	msgWithKey.Data = msg.Payload
	msgWithKey.Header = msg.Header
	pubOpts := make([]nats.PubOpt, 0, 1)
	if len(msgId) > 0 {
		pubOpts = append(pubOpts, nats.MsgId(msgId))
	}

	// // 异步发送消息, 例子见 https://docs.nats.io/using-nats/developer/develop_jetstream/publish
	// ackFuture, err := that.js.PublishMsgAsync(msgWithKey, pubOpts...)
	// if err != nil {
	// 	if that.logf != nil {
	// 		that.logf(klog.WarnLevel, natsmq_tag, "Publish error: {}", err)
	// 	}

	// 	if that.conf.OnError() != nil {
	// 		that.conf.OnError()(err)
	// 	}
	// 	return
	// }
	// select {
	// case <-ackFuture.Ok():
	// 	if that.logf != nil {
	// 		that.logf(klog.WarnLevel, natsmq_tag, "Publish ok: {}", ackFuture)
	// 	}
	// case err := <-ackFuture.Err():
	// 	if that.logf != nil {
	// 		that.logf(klog.WarnLevel, natsmq_tag, "Publish error: {}", err)
	// 	}
	// }

	// 同步发送消息
	ack, err := that.js.PublishMsg(msgWithKey, pubOpts...)
	if err != nil {
		if that.logf != nil {
			that.logf(klog.WarnLevel, natsmq_tag, "Publish error: {}", err)
		}

		if that.conf.OnError() != nil {
			that.conf.OnError()(err)
		}
	} else {
		// id重复, 被服务器忽略了, 不会投递到订阅者中
		if ack.Duplicate {
			if that.logf != nil {
				that.logf(klog.WarnLevel, natsmq_tag, "Publish duplicate: {}", ack)
			}
		}
	}
}

//////////////////////////////////////////////////////////////
