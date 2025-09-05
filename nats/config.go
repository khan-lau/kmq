package nats

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/khan-lau/kutils/container/klists"
	"github.com/nats-io/nats.go"
)

const (
	natsmq_tag = "natsmq"
)

type SubscribeCallback func(voidObj interface{}, msg *NatsMessage)

type ErrorCallbackFunc func(err error)
type EventCallbackFunc func(event interface{})

type NatsMessage struct {
	Seq     int64       // 消息序列号, 仅在 JetStream 模式下有效, 实际存储的是纳秒时间戳
	Topic   string      // 主题
	Reply   string      // 回复主题 reply模式时有效
	Header  nats.Header // 消息头信息
	Payload []byte      // 消息体内容

	origin *nats.Msg
}

// 如果想批量确认 需要将 AckPolicy设置为 `AckAllPolicy`
func (that *NatsMessage) Ack() error {
	if that.origin == nil {
		// return kstrings.Errorf("origin is nil")
		return nil
	}
	return that.origin.Ack()
}

//////////////////////////////////////////////////////////////

// 默认值
const (
	defaultPingInterval = 2 * 60 * 1000 // 2分钟
	defaultMaxPingsOut  = 3
)

// 默认值
const (
	defaultMaxWait      = 30000 // 30秒
	defaultBatchSize    = 128
	defaultMaxQueueSize = 1024
)

// 默认值
const (
	defaultMaxConsumers = -1
	defaultMaxMsgSize   = -1
	defaultDuplicates   = 120000 //多长时间内不允许消息重复, 单位MS, 默认为 120000 (默认 2 minutes)

)

//////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////

// NatsConfig 定义 NATS 客户端连接参数
type NatsConnConfig struct {
	servers  *klists.KList[string] // 服务器地址列表，支持多个地址, 如 "nats://127.0.0.1:4222,nats://127.0.0.1:4223,tls://127.0.0.1:4224"
	user     string                // 用户名可以为空
	password string                // 密码或token

	allowReconnect   bool          // 默认不启用自动重连
	maxReconnect     int           // 最大重连次数，0 表示不启用自动重连
	reconnectWait    time.Duration // 重连等待时间，单位毫秒
	reconnectBufSize int           // 在客户端与服务器连接断开时，临时缓存你发布的出站（outgoing）消息, -1 不启用自动重连缓冲区, 默认8M缓冲区
	connectTimeout   time.Duration // 连接超时时间，单位为毫秒

	useTls             bool   // 默认不启用 TLS 加密连接
	caCertPath         string // CA 证书路径, 仅当 useTLS 为 true 时有效
	tlsClientCert      string // 客户端证书路径, 仅当 useTLS 为 true 时有效
	keyPath            string // 密钥表路径, 仅当 useTLS 为 true 时有效
	insecureSkipVerify bool   // 忽略证书验证，默认为 false
	minTlsVer          int    // 最小TLS版本，默认为1.2 VersionTLS10 = 0x0301, VersionTLS11 = 0x0302, VersionTLS12 = 0x0303, VersionTLS13 = 0x0304

	name string // 客户端名称，默认为空

	pingInterval time.Duration // ping间隔时间, 单位毫秒, 默认为2分钟
	maxPingsOut  int           // 最大允许的ping无应答次数, 超过则断开连接
}

func NewNatsConnConfig(name string) *NatsConnConfig {
	return &NatsConnConfig{
		servers:            klists.New[string](),
		allowReconnect:     false,
		maxReconnect:       0,
		reconnectWait:      0,
		reconnectBufSize:   -1, // 默认不启用自动重连缓冲
		connectTimeout:     0,
		useTls:             false,
		caCertPath:         "",
		tlsClientCert:      "",
		keyPath:            "",
		insecureSkipVerify: true,
		minTlsVer:          tls.VersionTLS10,
		name:               name,
		pingInterval:       defaultPingInterval,
		maxPingsOut:        defaultMaxPingsOut,
	}
}

// Getter 方法
func (that *NatsConnConfig) Servers() []string             { return klists.ToKSlice(that.servers) }
func (that *NatsConnConfig) User() string                  { return that.user }
func (that *NatsConnConfig) Password() string              { return that.password }
func (that *NatsConnConfig) AllowReconnect() bool          { return that.allowReconnect }
func (that *NatsConnConfig) MaxReconnect() int             { return that.maxReconnect }
func (that *NatsConnConfig) ReconnectWait() time.Duration  { return that.reconnectWait }
func (that *NatsConnConfig) ReconnectBufSize() int         { return that.reconnectBufSize }
func (that *NatsConnConfig) ConnectTimeout() time.Duration { return that.connectTimeout }
func (that *NatsConnConfig) UseTls() bool                  { return that.useTls }
func (that *NatsConnConfig) CaCertPath() string            { return that.caCertPath }
func (that *NatsConnConfig) TlsClientCert() string         { return that.tlsClientCert }
func (that *NatsConnConfig) KeyPath() string               { return that.keyPath }
func (that *NatsConnConfig) InsecureSkipVerify() bool      { return that.insecureSkipVerify }
func (that *NatsConnConfig) MinTlsVer() int                { return that.minTlsVer }
func (that *NatsConnConfig) Name() string                  { return that.name }
func (that *NatsConnConfig) PingInterval() time.Duration   { return that.pingInterval }
func (that *NatsConnConfig) MaxPingsOut() int              { return that.maxPingsOut }

// Setter 方法（链式调用，非法值使用默认值）
func (that *NatsConnConfig) AddServers(servers ...string) *NatsConnConfig {
	for _, url := range servers {
		if strings.HasPrefix(url, "nats://") || strings.HasPrefix(url, "tls://") {
			that.servers.PushBack(url)
		}
	}
	return that
}

func (that *NatsConnConfig) SetUserPassword(user string, password string) *NatsConnConfig {
	that.user = user         // 用户名可以为空
	that.password = password // 密码可以为空
	return that
}

func (that *NatsConnConfig) SetToken(token string) *NatsConnConfig {
	that.password = token // 密码可以为空
	return that
}

func (that *NatsConnConfig) EnableReconnect(maxReConnect, reconnectBufSize int, timeout, reconnectWait time.Duration) *NatsConnConfig {
	that.allowReconnect = true
	that.maxReconnect = maxReConnect
	that.connectTimeout = timeout
	that.reconnectWait = reconnectWait
	that.reconnectBufSize = reconnectBufSize
	return that
}

func (that *NatsConnConfig) DisableReconnect() *NatsConnConfig {
	that.allowReconnect = false
	that.maxReconnect = 0
	that.connectTimeout = 0
	that.reconnectWait = 0
	that.reconnectBufSize = -1
	return that
}

func (that *NatsConnConfig) EnableTls(caCertPath, tlsClientCert, keyPath string, insecureSkipVerify bool, minTlsVer int) *NatsConnConfig {
	that.useTls = true
	validVersions := map[int]bool{
		tls.VersionTLS10: true, tls.VersionTLS11: true, tls.VersionTLS12: true, tls.VersionTLS13: true,
	}
	if !validVersions[minTlsVer] {
		fmt.Printf("Warning: invalid TLS version %x, using default: tls.VersionTLS10\n", minTlsVer)
		that.minTlsVer = tls.VersionTLS10
	} else {
		that.minTlsVer = minTlsVer
	}

	that.caCertPath = caCertPath
	that.tlsClientCert = tlsClientCert
	that.keyPath = keyPath
	that.insecureSkipVerify = insecureSkipVerify

	return that
}

func (that *NatsConnConfig) DisableTls() *NatsConnConfig {
	that.useTls = false
	that.caCertPath = ""
	that.tlsClientCert = ""
	that.keyPath = ""
	that.insecureSkipVerify = false
	that.minTlsVer = tls.VersionTLS10 // VersionTLS12
	return that
}

func (that *NatsConnConfig) SetName(name string) *NatsConnConfig {
	that.name = name // 名称可以为空
	return that
}

func (that *NatsConnConfig) SetPing(interval time.Duration, retry int) *NatsConnConfig {
	if interval < 0 {
		fmt.Printf("Warning: pingInterval %d is invalid, using default: %dms\n", interval, defaultPingInterval)
		that.pingInterval = defaultPingInterval
	} else {
		that.pingInterval = interval
	}

	if retry < 1 {
		fmt.Printf("Warning: maxPingsOut %d is invalid, using default: %d\n", retry, defaultMaxPingsOut)
		that.maxPingsOut = defaultMaxPingsOut
	} else {
		that.maxPingsOut = retry
	}
	return that
}

//////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////

// CoreNatsConfig 定义 Core NATS 模式订阅配置
type CoreNatsConfig struct {
	topics     *klists.KList[string] // 主题列表
	queueGroup string                // 消费组名称, 用于负载均衡, 允许空字符串, 为空时为广播模式, 非空时为负载均衡模式, core模式下 没有 类似kafka的通过key的hash实现的负载均衡模式
	maxPending int                   // 最大等待的消息数量，默认为 0 (无限)
}

func NewCoreNatsConfig() *CoreNatsConfig {
	return &CoreNatsConfig{
		topics:     klists.New[string](), // 主题列表
		queueGroup: "",                   // 消费组名称, 用于负载均衡, 允许空字符串
		maxPending: 0,                    // 最大等待的消息数量，默认为 0 (无限)
	}
}

// Getter 方法
func (that *CoreNatsConfig) Topics() []string   { return klists.ToKSlice(that.topics) }
func (that *CoreNatsConfig) QueueGroup() string { return that.queueGroup }
func (that *CoreNatsConfig) MaxPending() int    { return that.maxPending }

// Setter 方法
func (that *CoreNatsConfig) AddTopics(topics ...string) *CoreNatsConfig {
	for _, topic := range topics {
		if len(topic) > 0 {
			that.topics.PushBackSlice(topic)
		}
	}

	return that
}

func (that *CoreNatsConfig) SetQueueGroup(group string) *CoreNatsConfig {
	that.queueGroup = group // 队列组可以为空
	return that
}

func (that *CoreNatsConfig) SetMaxPending(max int) *CoreNatsConfig {
	if max < 0 {
		fmt.Printf("Warning: maxPending %d is invalid, using default: 0\n", max)
		that.maxPending = 0
	} else {
		that.maxPending = max
	}
	return that
}

//////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////

// JetStreamConsumerConfig 定义 JetStream Consumer 配置
type JetStreamConsumerConfig struct {
	durable string // 持久化订阅名称，可以为空, 等同于消费者名称, 为空时表示临时消费组, 最后一个消费者断开连接时, 未处理消息被丢弃
	// filterSubject string // 过滤主题，可以为空
	maxWait    int  // 最大等待时间，默认为 -1 (无限)
	autoCommit bool // 是否自动提交，默认为 true
	// batchSize     int                // 批量消费的大小，默认为 -1 (无限)
	// maxQueueSize  int                // 最大队列大小，默认为 -1 (无限)
	ackPolicy     nats.AckPolicy     // 确认策略，默认`AckNonePolicy`无需ack, AckAllPolicy 批量ack所有消息; AckExplicitPolicy 需要显式ack每个消息
	deliverPolicy nats.DeliverPolicy // 投递策略，默认为 DeliverAllPolicy, 从第一条开始消费; DeliverLastPolicy 从最后一条开始消费; DeliverNewPolicy 最新一条开始;  DeliverByStartSequencePolicy 从指定序列开始; DeliverByStartTimePolicy 从指定时间开始; DeliverLastPerSubjectPolicy 从每个主题的最后一条开始
}

func NewJetStreamConsumerConfig(name string) *JetStreamConsumerConfig {
	return &JetStreamConsumerConfig{
		durable: name,
		// filterSubject: "",
		maxWait: -1,
		// batchSize:     -1,
		// maxQueueSize:  -1,
		ackPolicy:     nats.AckNonePolicy,            // 默认无需ack
		deliverPolicy: nats.DeliverByStartTimePolicy, // 默认从指定时间开始消费
	}
}

// Getter 方法
func (that *JetStreamConsumerConfig) Name() string { return that.durable }

// func (that *JetStreamConsumerConfig) FilterSubject() string { return that.filterSubject }
func (that *JetStreamConsumerConfig) MaxWait() int     { return that.maxWait }
func (that *JetStreamConsumerConfig) AutoCommit() bool { return that.autoCommit }

// func (that *JetStreamConsumerConfig) BatchSize() int                    { return that.batchSize }
// func (that *JetStreamConsumerConfig) MaxQueueSize() int                 { return that.maxQueueSize }
func (that *JetStreamConsumerConfig) AckPolicy() nats.AckPolicy         { return that.ackPolicy }
func (that *JetStreamConsumerConfig) DeliverPolicy() nats.DeliverPolicy { return that.deliverPolicy }

// Setter 方法
func (that *JetStreamConsumerConfig) SetName(name string) *JetStreamConsumerConfig {
	if name == "" {
		fmt.Printf("Warning: durable name is empty\n")
	}
	that.durable = name
	return that
}

// func (that *JetStreamConsumerConfig) SetFilterSubject(subject string) *JetStreamConsumerConfig {
// 	that.filterSubject = subject // 可以为空
// 	return that
// }

func (that *JetStreamConsumerConfig) SetMaxWait(wait int) *JetStreamConsumerConfig {
	if wait <= 0 {
		fmt.Printf("Warning: maxWait %d is invalid, using default: %d\n", wait, defaultMaxWait)
		that.maxWait = defaultMaxWait
	} else {
		that.maxWait = wait
	}
	return that
}

func (that *JetStreamConsumerConfig) SetAutoCommit(commit bool) *JetStreamConsumerConfig {
	that.autoCommit = commit
	return that
}

// func (that *JetStreamConsumerConfig) SetBatchSize(size int) *JetStreamConsumerConfig {
// 	if size <= 0 {
// 		fmt.Printf("Warning: batchSize %d is invalid, using default: %d\n", size, defaultBatchSize)
// 		that.batchSize = defaultBatchSize
// 	} else {
// 		that.batchSize = size
// 	}
// 	return that
// }

// func (that *JetStreamConsumerConfig) SetMaxQueueSize(size int) *JetStreamConsumerConfig {
// 	if size <= 0 {
// 		fmt.Printf("Warning: maxQueueSize %d is invalid, using default: %d\n", size, defaultMaxQueueSize)
// 		that.maxQueueSize = defaultMaxQueueSize
// 	} else {
// 		that.maxQueueSize = size
// 	}
// 	return that
// }

func (that *JetStreamConsumerConfig) SetAckPolicy(policy nats.AckPolicy) *JetStreamConsumerConfig {
	that.ackPolicy = policy
	return that
}

func (that *JetStreamConsumerConfig) SetDeliverPolicy(policy nats.DeliverPolicy) *JetStreamConsumerConfig {
	that.deliverPolicy = policy
	return that
}

func (that *JetStreamConsumerConfig) AckPolicyFromStr(policy string) nats.AckPolicy {
	switch policy {
	case "all":
		return nats.AckAllPolicy
	case "explicit":
		return nats.AckExplicitPolicy
	case "none":
		return nats.AckNonePolicy
	default:
		return nats.AckNonePolicy
	}
}

func (that *JetStreamConsumerConfig) DeliverPolicyFromStr(policy string) nats.DeliverPolicy {
	switch policy {
	case "all":
		return nats.DeliverAllPolicy
	case "last":
		return nats.DeliverLastPolicy
	case "new":
		return nats.DeliverNewPolicy
	case "start_sequence":
		return nats.DeliverByStartSequencePolicy
	case "start_time":
		return nats.DeliverByStartTimePolicy
	case "last_per_subject":
		return nats.DeliverLastPerSubjectPolicy
	default:
		return nats.DeliverAllPolicy
	}
}

//////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////

// JetStreamConfig 定义 JetStream Stream 配置
type JetStreamConfig struct {
	name               string                   // Stream 名称，必须唯一, 且不可为空
	storageType        nats.StorageType         // 存储类型，默认为 "memory", 可选 "file"
	storageCompression nats.StoreCompression    // 存储压缩，默认为 "none", 可选 "s2"
	retentionPolicy    nats.RetentionPolicy     // 保留策略，默认为 "limits", 可选 "interest" 或 "workqueue"
	maxConsumers       int                      // 最大消费者数，默认为 -1 (无限)
	maxMsgs            int64                    // 最大消息数，默认为 -1 (无限)
	maxBytes           int64                    // 最大字节数，默认为 -1 (无限)
	maxAge             int64                    // 最大消息年龄，默认为 -1 (无限)
	maxMsgsPerSubject  int64                    // 每个主题的最大消息数，默认为 -1 (无限)
	maxMsgSize         int32                    // 最大消息大小，默认为 -1 (无限)
	duplicates         int64                    // 多长时间内不允许消息重复, 单位MS, 默认为 -1 (无限)
	discard            nats.DiscardPolicy       // 丢弃策略，默认为 "old", 可选 "new" 或 "none"
	topics             *klists.KList[string]    // 主题列表，默认为空, nats支持一个stream对多个consmer, 但此处只实现了一个stream对应一个consumer
	consumer           *JetStreamConsumerConfig // 消费者配置列表，默认为空
}

// Getter 方法
func (that *JetStreamConfig) Name() string                          { return that.name }
func (that *JetStreamConfig) StorageType() nats.StorageType         { return that.storageType }
func (that *JetStreamConfig) Compression() nats.StoreCompression    { return that.storageCompression }
func (that *JetStreamConfig) RetentionPolicy() nats.RetentionPolicy { return that.retentionPolicy }
func (that *JetStreamConfig) MaxConsumers() int                     { return that.maxConsumers }
func (that *JetStreamConfig) MaxMsgs() int64                        { return that.maxMsgs }
func (that *JetStreamConfig) MaxBytes() int64                       { return that.maxBytes }
func (that *JetStreamConfig) MaxAge() int64                         { return that.maxAge }
func (that *JetStreamConfig) MaxMsgsPerSubject() int64              { return that.maxMsgsPerSubject }
func (that *JetStreamConfig) MaxMsgSize() int32                     { return that.maxMsgSize }
func (that *JetStreamConfig) Duplicates() int64                     { return that.duplicates }
func (that *JetStreamConfig) Discard() nats.DiscardPolicy           { return that.discard }
func (that *JetStreamConfig) Topics() []string                      { return klists.ToKSlice(that.topics) }
func (that *JetStreamConfig) Consumer() *JetStreamConsumerConfig    { return that.consumer }

func NewJetStreamConfig(name string) *JetStreamConfig {
	return &JetStreamConfig{
		name:              name,
		storageType:       nats.MemoryStorage,
		retentionPolicy:   nats.LimitsPolicy,
		maxConsumers:      defaultMaxConsumers,
		maxMsgs:           -1,
		maxBytes:          -1,
		maxAge:            -1,
		maxMsgsPerSubject: -1,
		maxMsgSize:        defaultMaxMsgSize,
		duplicates:        defaultDuplicates,
		discard:           nats.DiscardOld,
		topics:            klists.New[string](),
		consumer:          nil, // 默认消费者配置
	}
}

// // Setter 方法
// func (that *JetStreamConfig) SetName(name string) *JetStreamConfig {
// 	if name == "" {
// 		fmt.Printf("Warning: name is empty\n")
// 	} else {
// 		that.name = name
// 	}
// 	return that
// }

func (that *JetStreamConfig) SetStorageType(storage nats.StorageType) *JetStreamConfig {
	that.storageType = storage
	return that
}

func (that *JetStreamConfig) SetCompression(compression nats.StoreCompression) *JetStreamConfig {
	that.storageCompression = compression
	return that
}

func (that *JetStreamConfig) SetRetentionPolicy(policy nats.RetentionPolicy) *JetStreamConfig {
	that.retentionPolicy = policy
	return that
}

func (that *JetStreamConfig) SetRetentionLimits(maxMsgs, maxBytes, maxAge, maxMsgsPerSubject int64) *JetStreamConfig {
	if maxMsgs < 0 {
		maxMsgs = -1
	}
	if maxBytes < 0 {
		maxBytes = -1
	}
	if maxAge < 0 {
		maxAge = -1
	}
	if maxMsgsPerSubject < 0 {
		maxMsgsPerSubject = -1
	}

	that.retentionPolicy = nats.LimitsPolicy
	that.maxMsgs = maxMsgs
	that.maxBytes = maxBytes
	that.maxAge = maxAge
	that.maxMsgsPerSubject = maxMsgsPerSubject
	return that
}

func (that *JetStreamConfig) SetMaxConsumers(max int) *JetStreamConfig {
	if max < -1 {
		fmt.Printf("Warning: maxConsumers %d is invalid, using default: %d\n", max, defaultMaxConsumers)
		that.maxConsumers = defaultMaxConsumers
	} else {
		that.maxConsumers = max
	}
	return that
}

func (that *JetStreamConfig) SetMaxMsgSize(size int32) *JetStreamConfig {
	if size < -1 {
		fmt.Printf("Warning: maxMsgSize %d is invalid, using default: %d\n", size, defaultMaxMsgSize)
		that.maxMsgSize = defaultMaxMsgSize
	} else {
		that.maxMsgSize = size
	}
	return that
}

func (that *JetStreamConfig) SetDuplicates(dup int64) *JetStreamConfig {
	if dup < 0 {
		fmt.Printf("Warning: duplicates %d is invalid, using default: %d\n", dup, defaultDuplicates)
		that.duplicates = defaultDuplicates
	} else {
		that.duplicates = dup
	}
	return that
}

func (that *JetStreamConfig) SetDiscard(discard nats.DiscardPolicy) *JetStreamConfig {
	that.discard = discard
	return that
}

func (that *JetStreamConfig) AddTopic(topics ...string) *JetStreamConfig {
	for _, topic := range topics {
		if len(topic) > 0 {
			that.topics.PushBack(topic)
		}
	}
	return that
}

func (that *JetStreamConfig) SetConsumer(consumer *JetStreamConsumerConfig) *JetStreamConfig {
	that.consumer = consumer
	return that
}

func (that *JetStreamConfig) StorageTypeFromStr(typeStr string) nats.StorageType {
	switch typeStr {
	case "file":
		return nats.FileStorage
	case "memory":
		return nats.MemoryStorage
	default:
		return nats.MemoryStorage
	}
}

func (that *JetStreamConfig) StorageCompressionFromStr(compressStr string) nats.StoreCompression {
	switch compressStr {
	case "s2":
		return nats.S2Compression
	default:
		return nats.NoCompression
	}
}

func (that *JetStreamConfig) RetentionPolicyFromStr(policy string) nats.RetentionPolicy {
	switch policy {
	case "limits":
		return nats.LimitsPolicy
	case "work_queue":
		return nats.WorkQueuePolicy
	case "interests":
		return nats.InterestPolicy
	default:
		return nats.LimitsPolicy
	}
}

func (that *JetStreamConfig) DiscardFromStr(policy string) nats.DiscardPolicy {
	switch policy {
	case "old":
		return nats.DiscardOld
	case "new":
		return nats.DiscardNew
	default:
		return nats.DiscardOld
	}
}

//////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////

// NatsClientConfig 统一配置类
type NatsClientConfig struct {
	nats      *NatsConnConfig
	coreNats  *CoreNatsConfig
	jetStream *JetStreamConfig

	onError ErrorCallbackFunc // 设置错误回调
	onExit  EventCallbackFunc // 设置退出回调
}

func NewNatsClientConfig() *NatsClientConfig {
	return &NatsClientConfig{
		nats:      NewNatsConnConfig(""),
		coreNats:  nil,
		jetStream: nil,
		onError:   nil,
		onExit:    nil,
	}
}

// Getter 方法
func (that *NatsClientConfig) Nats() *NatsConnConfig       { return that.nats }
func (that *NatsClientConfig) CoreNats() *CoreNatsConfig   { return that.coreNats }
func (that *NatsClientConfig) JetStream() *JetStreamConfig { return that.jetStream }
func (that *NatsClientConfig) OnError() ErrorCallbackFunc  { return that.onError }
func (that *NatsClientConfig) OnExit() EventCallbackFunc   { return that.onExit }

// Setter 方法
func (that *NatsClientConfig) SetNats(nats *NatsConnConfig) *NatsClientConfig {
	that.nats = nats
	return that
}

func (that *NatsClientConfig) SetCoreNats(core *CoreNatsConfig) *NatsClientConfig {
	that.coreNats = core
	that.jetStream = nil // 初始化 JetStream, 与 CoreNats 配置互斥
	return that
}

func (that *NatsClientConfig) SetJetStream(js *JetStreamConfig) *NatsClientConfig {
	that.jetStream = js
	that.coreNats = nil // 初始化 CoreNats, 与 JetStream 配置互斥
	return that
}

func (that *NatsClientConfig) SetOnError(onError ErrorCallbackFunc) *NatsClientConfig {
	that.onError = onError
	return that
}

func (that *NatsClientConfig) SetOnExit(onExit EventCallbackFunc) *NatsClientConfig {
	that.onExit = onExit
	return that
}

//////////////////////////////////////////////////////////////
