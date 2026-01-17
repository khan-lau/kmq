package redismq

const (
	redis_tag = "redismq"
)

/////////////////////////////////////////////////////////////

type ErrorCallbackFunc func(err error)
type EventCallbackFunc func(event interface{})
type ReadyCallbackFunc func(ready bool)

/////////////////////////////////////////////////////////////

type RedisConfig struct {
	Host     string `json:"host"`
	Port     uint16 `json:"port"`
	Password string `json:"password"`
	DB       int    `json:"db"`
	Retry    int    `json:"retry"`

	Topics []string `json:"topics"`

	OnError ErrorCallbackFunc // 设置错误回调
	OnExit  EventCallbackFunc // 设置退出回调
	OnReady ReadyCallbackFunc // 设置启动完成回调
}

func NewRedisConfig() *RedisConfig {
	return &RedisConfig{
		Host:     "127.0.0.1",
		Port:     6379,
		Password: "",
		DB:       0,
		Retry:    5,
		Topics:   []string{},
	}
}

func (that *RedisConfig) SetHost(host string) *RedisConfig {
	that.Host = host
	return that
}

func (that *RedisConfig) SetPort(port uint16) *RedisConfig {
	that.Port = port
	return that
}

func (that *RedisConfig) SetPassword(password string) *RedisConfig {
	that.Password = password
	return that
}

func (that *RedisConfig) SetDB(db int) *RedisConfig {
	that.DB = db
	return that
}

func (that *RedisConfig) SetRetry(retry int) *RedisConfig {
	that.Retry = retry
	return that
}

func (that *RedisConfig) SetTopics(topics ...string) *RedisConfig {
	that.Topics = topics
	return that
}

func (that *RedisConfig) AddTopic(topic string) *RedisConfig {
	that.Topics = append(that.Topics, topic)
	return that
}

func (that *RedisConfig) SetErrorCallback(callback ErrorCallbackFunc) *RedisConfig {
	that.OnError = callback
	return that
}

func (that *RedisConfig) SetExitCallback(callback EventCallbackFunc) *RedisConfig {
	that.OnExit = callback
	return that
}

func (that *RedisConfig) SetReadyCallback(callback ReadyCallbackFunc) *RedisConfig {
	that.OnReady = callback
	return that
}
