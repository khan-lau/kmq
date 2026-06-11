package config

///////////////////////////////////////////////////////////

type NatsCoreConfig struct {
	ClientID           string   `json:"clientId" toml:"clientId" yaml:"clientId"`                               // 客户端ID，用于唯一标识一个NATS连接
	User               string   `json:"user" toml:"user" yaml:"user"`                                           // 用户名可以为空
	Password           string   `json:"password" toml:"password" yaml:"password"`                               // 密码或token
	BrokerList         []string `json:"brokerList" toml:"brokerList" yaml:"brokerList"`                         // 服务器地址列表，支持多个地址, 如 "nats://127.0.0.1:4222,nats://127.0.0.1:4223,tls://127.0.0.1
	AllowReconnect     bool     `json:"allowReconnect" toml:"allowReconnect" yaml:"allowReconnect"`             // 默认不启用自动重连
	MaxReconnect       int      `json:"maxReconnect" toml:"maxReconnect" yaml:"maxReconnect"`                   // 最大重连次数，0 表示不启用自动重连
	ReconnectWait      int64    `json:"reconnectWait" toml:"reconnectWait" yaml:"reconnectWait"`                // 重连等待时间，单位毫秒
	ReconnectBufSize   int      `json:"reconnectBufSize" toml:"reconnectBufSize" yaml:"reconnectBufSize"`       // 在客户端与服务器连接断开时，临时缓存你发布的出站（outgoing）消息, -1 不启用自动重连缓冲区, 默认8M缓冲区
	ConnectTimeout     int64    `json:"connectTimeout" toml:"connectTimeout" yaml:"connectTimeout"`             // 连接超时时间，单位为毫秒
	PingInterval       int64    `json:"pingInterval" toml:"pingInterval" yaml:"pingInterval"`                   // ping间隔时间, 单位毫秒, 默认为2分钟
	MaxPingsOut        int      `json:"maxPingsOut" toml:"maxPingsOut" yaml:"maxPingsOut"`                      // 最大允许的ping无应答次数, 超过则断开连接
	UseTls             bool     `json:"useTls" toml:"useTls" yaml:"useTls"`                                     // 默认不启用 TLS 加密连接
	CaCertPath         string   `json:"caCertPath" toml:"caCertPath" yaml:"caCertPath"`                         // CA 证书路径, 仅当 useTLS 为 true 时有效
	TlsClientCert      string   `json:"tlsClientCert" toml:"tlsClientCert" yaml:"tlsClientCert"`                // 客户端证书路径, 仅当 useTLS 为 true 时有效
	KeyPath            string   `json:"keyPath" toml:"keyPath" yaml:"keyPath"`                                  // 密钥表路径, 仅当 useTLS 为 true 时有效
	InsecureSkipVerify bool     `json:"insecureSkipVerify" toml:"insecureSkipVerify" yaml:"insecureSkipVerify"` // 忽略证书验证，默认为 false
	MinTlsVer          int      `json:"minTlsVer" toml:"minTlsVer" yaml:"minTlsVer"`                            // 最小TLS版本，默认为1.2 VersionTLS10 = 0x0301, VersionTLS11 = 0x0302, VersionTLS12 = 0x0303, VersionTLS13 = 0x0304

	Topics     []string `json:"topics" toml:"topics" yaml:"topics"`             // 主题列表
	QueueGroup string   `json:"queueGroup" toml:"queueGroup" yaml:"queueGroup"` // 消费组名称, 用于负载均衡, 允许空字符串, 为空时为广播模式, 非空时为负载均衡模式, core模式下 没有 类似kafka的通过key的hash实现的负载均衡模式
	MaxPending int      `json:"maxPending" toml:"maxPending" yaml:"maxPending"` // 最大等待的消息数量，默认为 0 (无限)
}

///////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////

type NatsJsConsumerConfig struct {
	GroupId            string `json:"groupId" toml:"groupId" yaml:"groupId"`          // 持久化订阅名称，可以为空, 等同于消费者名称, 为空时表示临时消费组, 最后一个消费者断开连接时, 未处理消息被丢弃
	MaxWait            int    `json:"maxWait" toml:"maxWait" yaml:"maxWait"`          // 最大等待时间，默认为 -1 (无限)
	StartWithTimestamp int64  `mapstructure:"-" json:"-"`                             // 最后消费的时间戳, 精度为纳秒, 默认为 -1 (无效)
	AutoCommit         string `json:"autoCommit" toml:"autoCommit" yaml:"autoCommit"` // 自动commit, 支持 native:原生自动提交, custom: 客户端实现自动提交, none: 手动提交
	AckPolicy          string `json:"ackPolicy" toml:"ackPolicy" yaml:"ackPolicy"`    // 确认策略，默认`none`: 自动ack; `all`: 确认一个序列号时，会隐式确认该序列号之前的所有消息; `explicit`: 每条需要单独确认
	DeliverPolicy      string `mapstructure:"-" json:"-"`                             // `json:"deliverPolicy" toml:"deliverPolicy" yaml:"deliverPolicy"` // 投递策略，固定为 `by_start_time`
	// `all`, 从第一条开始消费;  与 new不同
	// `last` 从最后一条开始消费;
	// `new` 最新一条开始, 从创建消费开始的第一条;
	// `by_start_sequence` 从指定序列号消费;
	// `by_start_time`; 从指定时间戳消费;
	// `last_per_subject`: 从每个主题的最后一条开始消费
}

type NatsJsConfig struct {
	ClientID           string   `json:"clientId" toml:"clientId" yaml:"clientId"`                               // 客户端ID，用于唯一标识一个NATS连接
	User               string   `json:"user" toml:"user" yaml:"user"`                                           // 用户名可以为空
	Password           string   `json:"password" toml:"password" yaml:"password"`                               // 密码或token
	BrokerList         []string `json:"brokerList" toml:"brokerList" yaml:"brokerList"`                         // 服务器地址列表，支持多个地址, 如 "nats://127.0.0.1:4222,nats://127.0.0.1:4223,tls://127.0.0.1
	AllowReconnect     bool     `json:"allowReconnect" toml:"allowReconnect" yaml:"allowReconnect"`             // 默认不启用自动重连
	MaxReconnect       int      `json:"maxReconnect" toml:"maxReconnect" yaml:"maxReconnect"`                   // 最大重连次数，0 表示不启用自动重连
	ReconnectWait      int64    `json:"reconnectWait" toml:"reconnectWait" yaml:"reconnectWait"`                // 重连等待时间，单位毫秒
	ReconnectBufSize   int      `json:"reconnectBufSize" toml:"reconnectBufSize" yaml:"reconnectBufSize"`       // 在客户端与服务器连接断开时，临时缓存你发布的出站（outgoing）消息, -1 不启用自动重连缓冲区, 默认8M缓冲区
	ConnectTimeout     int64    `json:"connectTimeout" toml:"connectTimeout" yaml:"connectTimeout"`             // 连接超时时间，单位为毫秒
	PingInterval       int64    `json:"pingInterval" toml:"pingInterval" yaml:"pingInterval"`                   // ping间隔时间, 单位毫秒, 默认为2分钟
	MaxPingsOut        int      `json:"maxPingsOut" toml:"maxPingsOut" yaml:"maxPingsOut"`                      // 最大允许的ping无应答次数, 超过则断开连接
	UseTls             bool     `json:"useTls" toml:"useTls" yaml:"useTls"`                                     // 默认不启用 TLS 加密连接
	CaCertPath         string   `json:"caCertPath" toml:"caCertPath" yaml:"caCertPath"`                         // CA 证书路径, 仅当 useTLS 为 true 时有效
	TlsClientCert      string   `json:"tlsClientCert" toml:"tlsClientCert" yaml:"tlsClientCert"`                // 客户端证书路径, 仅当 useTLS 为 true 时有效
	KeyPath            string   `json:"keyPath" toml:"keyPath" yaml:"keyPath"`                                  // 密钥表路径, 仅当 useTLS 为 true 时有效
	InsecureSkipVerify bool     `json:"insecureSkipVerify" toml:"insecureSkipVerify" yaml:"insecureSkipVerify"` // 忽略证书验证，默认为 false
	MinTlsVer          int      `json:"minTlsVer" toml:"minTlsVer" yaml:"minTlsVer"`                            // 最小TLS版本，默认为1.2 VersionTLS10 = 0x0301, VersionTLS11 = 0x0302, VersionTLS12 = 0x0303, VersionTLS13 = 0x0304

	QueueName          string                `json:"queueName" toml:"queueName" yaml:"queueName"`                            // 队列名称, jetstream名称, 必须不可为空, 命名不可包含 ` `, `.`, `*`, `>`, `\`, `/` 及非可见字符
	StorageType        string                `json:"storageType" toml:"storageType" yaml:"storageType"`                      // 存储类型，默认为 "memory", 可选 "file"
	StorageCompression string                `json:"storageCompression" toml:"storageCompression" yaml:"storageCompression"` // 存储压缩，默认为 "none", 可选 "s2"
	RetentionPolicy    string                `json:"retentionPolicy" toml:"retentionPolicy" yaml:"retentionPolicy"`          // 保留策略，默认为 "limits", 可选 "interest" 或 "workqueue"
	MaxConsumers       int                   `json:"maxConsumers" toml:"maxConsumers" yaml:"maxConsumers"`                   // 最大消费者数，默认为 -1 (无限)
	MaxMsgs            int64                 `json:"maxMsgs" toml:"maxMsgs" yaml:"maxMsgs"`                                  // 最大消息数，默认为 -1 (无限)
	MaxBytes           int64                 `json:"maxBytes" toml:"maxBytes" yaml:"maxBytes"`                               // 最大字节数，默认为 -1 (无限)
	MaxAge             int64                 `json:"maxAge" toml:"maxAge" yaml:"maxAge"`                                     // 最大消息生命周期，默认为 -1 (无限)
	MaxMsgsPerSubject  int64                 `json:"maxMsgsPerSubject" toml:"maxMsgsPerSubject" yaml:"maxMsgsPerSubject"`    // 每个主题的最大消息数，默认为 -1 (无限)
	MaxMsgSize         int32                 `json:"maxMsgSize" toml:"maxMsgSize" yaml:"maxMsgSize"`                         // 最大消息大小，默认为 -1 (无限)
	Duplicates         int64                 `json:"duplicates" toml:"duplicates" yaml:"duplicates"`                         // 多长时间内不允许消息重复, 单位MS, 默认为 -1 (无限)
	Discard            string                `json:"discard" toml:"discard" yaml:"discard"`                                  // 丢弃策略，默认为 "old", 可选 "new"
	Topics             []string              `json:"topics" toml:"topics" yaml:"topics"`                                     // 主题列表，默认为空, nats支持一个stream对多个consmer, 但此处只实现了一个stream对应一个consumer
	ConsumerConfig     *NatsJsConsumerConfig `json:"consumer" toml:"consumer" yaml:"consumer" mapstructure:"consumer"`       // 消费者配置
}
