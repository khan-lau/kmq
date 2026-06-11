package config

///////////////////////////////////////////////////////////

type RocketCustomConfig struct {
	Topics              []string `json:"topics" toml:"topics" yaml:"topics"`
	Mode                string   `json:"mode" toml:"mode" yaml:"mode"`                                              // BroadCasting 广播模式;  Clustering 集群模式; 默认为 Clustering
	Offset              string   `json:"offset" toml:"offset" yaml:"offset"`                                        // ConsumeFromFirstOffset 最新消息;  ConsumeFromLastOffset  最旧消息; ConsumeFromTimestamp 指定时间戳开始消费
	Timestamp           string   `json:"timestamp" toml:"timestamp" yaml:"timestamp"`                               // 指定时间戳开始消费, 格式 "20131223171201"
	Order               bool     `json:"order" toml:"order" yaml:"order"`                                           // 是否顺序消费, 默认为 false
	MessageBatchMaxSize int      `json:"messageBatchMaxSize" toml:"messageBatchMaxSize" yaml:"messageBatchMaxSize"` // 批量消费消息的最大数量, 默认为 1
	MaxReconsumeTimes   int      `json:"maxReconsumeTimes" toml:"maxReconsumeTimes" yaml:"maxReconsumeTimes"`       // 最大重消费次数, 默认为 -1
	AutoCommit          string   `json:"autoCommit" toml:"autoCommit" yaml:"autoCommit"`                            // 自动commit, 支持 native:原生自动提交, custom: 客户端实现自动提交, none: 手动提交
	Interceptor         string   `json:"interceptor" toml:"interceptor" yaml:"interceptor"`                         // 消息拦截器, 默认为空
}

type RocketProducerConfig struct {
	Topics        []string `json:"topics" toml:"topics" yaml:"topics"`
	Timeout       int      `json:"timeout" toml:"timeout" yaml:"timeout"`                   // 消息发送超时时间
	Retry         int      `json:"retry" toml:"retry" yaml:"retry"`                         // 消息发送重试次数
	QueueSelector string   `json:"queueSelector" toml:"queueSelector" yaml:"queueSelector"` // 消息队列选择策略, RandomQueueSelector 随机选择队列; RoundRobinQueueSelector 按照轮训方式选择队列; ManualQueueSelector 直接选择消息中配置的队列
	Interceptor   string   `json:"interceptor" toml:"interceptor" yaml:"interceptor"`       // 消息拦截器, 默认为空
	AsyncSend     bool     `json:"asyncSend" toml:"asyncSend" yaml:"asyncSend"`             // 是否异步发送消息, 默认为 false
}

type RocketConfig struct {
	NsResolver bool     `json:"nsResolver" toml:"nsResolver" yaml:"nsResolver"`
	Servers    []string `json:"servers" toml:"servers" yaml:"servers"`
	GroupName  string   `json:"groupName" toml:"groupName" yaml:"groupName"`
	ClientID   string   `json:"clientID" toml:"clientID" yaml:"clientID"`
	Namespace  string   `json:"namespace" toml:"namespace" yaml:"namespace"` // 命名空间

	AccessKey string `json:"accessKey" toml:"accessKey" yaml:"accessKey"`
	SecretKey string `json:"secretKey" toml:"secretKey" yaml:"secretKey"`

	Consumer *RocketCustomConfig   `json:"consumer" toml:"consumer" yaml:"consumer" mapstructure:"consumer"`
	Producer *RocketProducerConfig `json:"producer" toml:"producer" yaml:"producer" mapstructure:"producer"`
}
