package config

type RedisConfig struct {
	Addrs    []string `json:"addrs" toml:"addrs" yaml:"addrs"`          // IP地址:端口
	Retry    uint16   `json:"retry" toml:"retry" yaml:"retry"`          // 重连次数
	Password string   `json:"password" toml:"password" yaml:"password"` // 密码
	DbNum    uint8    `json:"dbNum" toml:"dbNum" yaml:"dbNum"`          // 数据库编号
	Topics   []string `json:"topics" toml:"topics" yaml:"topics"`       // 主题
}
