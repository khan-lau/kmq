package config

///////////////////////////////////////////////////////////

type RabbitConsumerConfig struct {
	QueueName  string `json:"queueName" toml:"queueName" yaml:"queueName"`    // 队列名称
	Exchange   string `json:"exchange" toml:"exchange" yaml:"exchange"`       // 交换机名称
	KRouterKey string `json:"kRouterKey" toml:"kRouterKey" yaml:"kRouterKey"` // 路由键
	WorkType   string `json:"workType" toml:"workType" yaml:"workType"`       // 工作模式
	AutoCommit string `json:"autoCommit" toml:"autoCommit" yaml:"autoCommit"` // 自动commit, 支持 native:原生自动提交, custom: 客户端实现自动提交, none: 手动提交
}

type RabbitProducerConfig struct {
	Exchange  string `json:"exchange" toml:"exchange" yaml:"exchange"`    // 交换机名称
	Router    string `json:"router" toml:"router" yaml:"router"`          // 路由键
	WorkType  string `json:"workType" toml:"workType" yaml:"workType"`    // 工作模式
	ReturnAck bool   `json:"returnAck" toml:"returnAck" yaml:"returnAck"` // 是否返回确认消息
}

type RabbitConfig struct {
	Addrs    []string `json:"addrs" toml:"addrs" yaml:"addrs"` // IP:Port
	User     string   `json:"user" toml:"user" yaml:"user"`
	Password string   `json:"password" toml:"password" yaml:"password"`
	VHost    string   `json:"vhost" toml:"vhost" yaml:"vhost"`

	Consumer *RabbitConsumerConfig `json:"consumer" toml:"consumer" yaml:"consumer" mapstructure:"consumer"` // 设置消费配置
	Producer *RabbitProducerConfig `json:"producer" toml:"producer" yaml:"producer" mapstructure:"producer"` // 设置生产配置
}
