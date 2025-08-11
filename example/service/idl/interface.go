package idl

// 服务状态定义
type ServiceStatus string

const (
	// 状态切换路径 1 stopped -> running
	// 状态切换路径 2 running -> stopped
	// 状态切换路径 3 running -> restarting -> running

	ServiceStatusStopped ServiceStatus = "stopped"
	ServiceStatusRunning ServiceStatus = "running"
	ServiceStatusRestart ServiceStatus = "restarting"
)

type OnRecived func(origin interface{}, name string, topic string, partition int, offset int64, properties map[string]string, message []byte)

type ServiceInterface interface {
	// 服务名称
	Name() string

	// 初始化服务
	Init()

	// // 异步启动服务
	// StartAsync()

	// 启动服务
	Start() error

	// 重启服务, 如果服务未启动, 则直接进行启动
	Restart() error

	// 停止服务, 如果服务未启动, 则不进行任何操作
	Stop() error
}
