package config

type RedisConfig struct {
	Addrs    []string `json:"addrs" toml:"addrs" yaml:"addrs" hcl:"addrs,attr"`             // IP地址:端口
	Retry    uint16   `json:"retry" toml:"retry" yaml:"retry" hcl:"retry,attr"`             // 重连次数
	Password string   `json:"password" toml:"password" yaml:"password" hcl:"password,attr"` // 密码
	DbNum    uint8    `json:"dbNum" toml:"dbNum" yaml:"dbNum" hcl:"dbNum,attr"`             // 数据库编号
	Topics   []string `json:"topics" toml:"topics" yaml:"topics" hcl:"topics,attr"`         // 主题
}
