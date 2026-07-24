package config

///////////////////////////////////////////////////////////

type RabbitConsumerConfig struct {
	QueueName  string `json:"queueName" toml:"queueName" yaml:"queueName" hcl:"queueName,attr"`     // 队列名称
	Exchange   string `json:"exchange" toml:"exchange" yaml:"exchange" hcl:"exchange,attr"`         // 交换机名称
	KRouterKey string `json:"kRouterKey" toml:"kRouterKey" yaml:"kRouterKey" hcl:"kRouterKey,attr"` // 路由键
	WorkType   string `json:"workType" toml:"workType" yaml:"workType" hcl:"workType,attr"`         // 工作模式
	AutoCommit string `json:"autoCommit" toml:"autoCommit" yaml:"autoCommit" hcl:"autoCommit,attr"` // 自动commit, 支持 native:原生自动提交, custom: 客户端实现自动提交, none: 手动提交
}

type RabbitProducerConfig struct {
	Exchange  string `json:"exchange" toml:"exchange" yaml:"exchange" hcl:"exchange,attr"`     // 交换机名称
	Router    string `json:"router" toml:"router" yaml:"router" hcl:"router,attr"`             // 路由键
	WorkType  string `json:"workType" toml:"workType" yaml:"workType" hcl:"workType,attr"`     // 工作模式
	ReturnAck bool   `json:"returnAck" toml:"returnAck" yaml:"returnAck" hcl:"returnAck,attr"` // 是否返回确认消息
}

type RabbitConfig struct {
	Addrs    []string `json:"addrs" toml:"addrs" yaml:"addrs" hcl:"addrs,attr"` // IP:Port
	User     string   `json:"user" toml:"user" yaml:"user" hcl:"user,attr"`
	Password string   `json:"password" toml:"password" yaml:"password" hcl:"password,attr"`
	VHost    string   `json:"vhost" toml:"vhost" yaml:"vhost" hcl:"vhost,attr"`

	Consumer *RabbitConsumerConfig `json:"consumer" toml:"consumer" yaml:"consumer" hcl:"consumer,attr" mapstructure:"consumer"` // 设置消费配置
	Producer *RabbitProducerConfig `json:"producer" toml:"producer" yaml:"producer" hcl:"producer,attr" mapstructure:"producer"` // 设置生产配置
}
