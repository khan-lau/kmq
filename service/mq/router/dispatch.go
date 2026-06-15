package router

import (
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/khan-lau/kmq/service/idl"
	"github.com/khan-lau/kmq/service/mq/target"
	"github.com/khan-lau/kutils/container/kcontext"
	klog "github.com/khan-lau/kutils/klogger"
	"github.com/khan-lau/kutils/ksync"
)

const (
	DispatchLogTag = "dispatch_service"
)

//////////////////////////////////////////////////////////////

type GenericMessage struct {
	Topic      string            `json:"topic"`
	Message    []byte            `json:"message"`
	Properties map[string]string `json:"properties"`
}

//////////////////////////////////////////////////////////////

//////////////////////////////////////////////////////////////

// 告警转发到下游MQ

type DispatchService struct {
	ctx    *kcontext.ContextNode
	name   string            // 服务名称
	status idl.ServiceStatus // 服务状态

	draining atomic.Bool // 排水状态管理

	queue        *ksync.LockedRingBuffer[*GenericMessage] // 消息队列
	queueSize    uint                                     // 消息队列大小
	timer        *time.Timer                              // 定时器
	sendInterval uint                                     // 发送间隔，毫秒
	dumpHex      bool                                     // 是否以十六进制形式打印消息内容

	mqTargets map[string]idl.ServiceInterface // 目标MQ服务列表

	mutex  sync.Mutex        // 互斥锁，用于保护共享资源
	buffer []*GenericMessage // 缓冲区，用于存储待发送的消息

	logf klog.AppLogFuncWithTag // 日志函数
	wg   sync.WaitGroup         // 等待组，用于等待排水完成
}

// NewDispatchService 创建一个新的 DispatchService 实例
//
// 参数:
//
//	ctx: 上下文节点，用于创建子上下文
//	dumpHex: 是否以十六进制形式打印消息内容
//	sendInterval: 消息发送间隔，单位毫秒
//	chanSize: 消息通道大小
//	maxBatchSize: 批量发送时, 单次消息最大条数, 小于等于1时, 不启用批量发送
//	name: 服务名称
//	mqTargets: 目标 MQ 服务列表，用于转发消息到不同的队列或主题
//	logf: 日志记录函数，带有标签的 AppLogFuncWithTag 类型
//
// 返回值:
//
//	*DispatchService: 指向新创建的 DispatchService 实例的指针
func NewDispatchService(ctx *kcontext.ContextNode, dumpHex bool, sendInterval uint, queueSize uint, maxBatchSize uint, name string, mqTargets map[string]idl.ServiceInterface, logf klog.AppLogFuncWithTag) *DispatchService {
	var timer *time.Timer
	if maxBatchSize > 1 {
		timer = time.NewTimer(time.Duration(sendInterval) * time.Millisecond)
	}

	queue, err := ksync.NewLockedRingBuffer[*GenericMessage](uint64(queueSize))
	if err != nil {
		if logf != nil {
			logf(klog.ErrorLevel, DispatchLogTag, "create ring buffer error: %v", err)
		}
		return nil
	}

	subCtx := ctx.NewChild(name)
	service := &DispatchService{
		ctx:          subCtx,
		name:         name,
		status:       idl.ServiceStatusStopped,
		draining:     atomic.Bool{},                            // 排水状态管理
		queue:        queue,                                    // 消息队列
		queueSize:    queueSize,                                // 消息队列大小
		timer:        timer,                                    // 定时器，用于触发消息发送, 毫秒
		sendInterval: sendInterval,                             // 发送间隔，毫秒
		dumpHex:      dumpHex,                                  // 是否以十六进制形式打印消息内容
		mqTargets:    mqTargets,                                // 目标MQ服务列表，用于转发消息到不同的队列或主题
		buffer:       make([]*GenericMessage, 0, maxBatchSize), // 缓冲区，用于存储待发送的消息
		logf:         logf,
		wg:           sync.WaitGroup{},
	}

	return service
}

func (that *DispatchService) Init() {}

func (that *DispatchService) Name() string {
	return that.name
}

func (that *DispatchService) StartAsync() {
	go func() {
		err := that.Start()
		if err != nil {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, DispatchLogTag, "start service %s error: %v", that.name, err)
			}
			that.onError(that.name, err)
		}

		if that.logf != nil {
			that.logf(klog.DebugLevel, DispatchLogTag, "service %s start async", that.name)
		}

		if that.logf != nil {
			that.logf(klog.DebugLevel, DispatchLogTag, "service %s start async done", that.name)
		}
	}()
}

func (that *DispatchService) Start() error {
	if that.status != idl.ServiceStatusStopped { //检查服务状态 是否为停止状态
		return fmt.Errorf("service %s is not stopped, status=%v", that.name, that.status)
	}

	if len(that.mqTargets) == 0 {
		return fmt.Errorf("service %s mqTargets is empty", that.name)
	} else {
		if that.logf != nil {
			that.logf(klog.DebugLevel, DispatchLogTag, "service %s mqTargets: %d", that.name, len(that.mqTargets))
		}
	}

	that.wg.Add(1) // 成员变量 wg 只负责追踪 Start 函数本身的生命周期
	defer that.wg.Done()

	that.draining.Store(false)

	var workerWg sync.WaitGroup // 使用局部 WaitGroup 控制子协程（搬运工）
	workerWg.Add(1)

	subCtx := that.ctx.NewChild(DispatchLogTag + "_start")
	go func(ctx *kcontext.ContextNode) {
		defer workerWg.Done()

		var timerCh <-chan time.Time // 创建一个 channel 变量来持有 timer.C, 防止timer 为 nil时导致崩溃
		if that.timer != nil {
			timerCh = that.timer.C
		}

		time.Sleep(time.Duration(5000) * time.Millisecond)
		idleCount := 0 // 引入空闲计数

	END_LOOP:
		for {
			select {
			case <-ctx.Context().Done():
				// 清理 buffer 残留
				that.mutex.Lock()
				if len(that.buffer) > 0 {
					lastBuff := make([]*GenericMessage, len(that.buffer))
					copy(lastBuff, that.buffer)
					that.buffer = that.buffer[:0]
					that.mutex.Unlock()
					that.sendArray(lastBuff)
				} else {
					that.mutex.Unlock()
				}

				drainBuffer := make([]*GenericMessage, that.queueSize)
				n := that.queue.DequeueToWait(drainBuffer, 5000*time.Millisecond)
				if n > 0 {
					for _, msg := range drainBuffer[:n] {
						that.send(msg) // 退出时建议直接单条发送，确保可靠性
					}
				}

				break END_LOOP
			case <-timerCh:
				// 处理定时器消息
				var buffCopy []*GenericMessage
				that.mutex.Lock()
				if len(that.buffer) > 0 { // 检查缓冲区是否为空
					buffCopy = make([]*GenericMessage, len(that.buffer)) // 创建一个新切片
					copy(buffCopy, that.buffer)
					that.buffer = that.buffer[:0] // 清空缓冲区
				}
				that.mutex.Unlock()
				if len(buffCopy) > 0 { // 检查缓冲区是否为空
					that.sendArray(buffCopy)
				}
				that.timer.Reset(time.Duration(that.sendInterval) * time.Millisecond) // 重置定时器，继续等待下一次触发

			default:
				if msg, ok := that.queue.TryDequeue(); ok {
					idleCount = 0 // 有数据，重置空闲计数

					// 处理消息
					// if that.logf != nil {
					// 	that.logf(klog.DebugLevel, DispatchLogTag, "service %s transform topic: %s message: %s", that.name, msg.Topic, string(msg.Message))
					// }

					tmpRatio := max(cap(that.buffer)/2, 1) // 计算缓冲区的一半，至少为1

					// 批量发送数组
					if tmpRatio > 1 {
						var buffCopy []*GenericMessage
						that.mutex.Lock()                 // 加锁
						if len(that.buffer) >= tmpRatio { // 检查缓冲区是否已满
							buffCopy = make([]*GenericMessage, len(that.buffer)) // 创建一个新切片
							copy(buffCopy, that.buffer)
							that.buffer = that.buffer[:0] // 清空缓冲区
						}
						that.buffer = append(that.buffer, msg) // 将消息添加到缓冲区
						that.mutex.Unlock()

						if len(buffCopy) > 0 { // 检查缓冲区是否为空
							that.sendArray(buffCopy)
						}
					} else {
						// 单条直接发送
						that.send(msg)
					}
				} else {
					// 没拿到数据，开始退避
					idleCount++ // 连续 1000 次没拿到数据（大概经过了微秒级的尝试）
					if idleCount > 1000 {
						time.Sleep(10 * time.Millisecond) // 进入休眠，防止 CPU 空转 100%, 这里设置 10ms 是延迟与功耗的平衡点
					}
				}
			}

		}
		if that.logf != nil {
			that.logf(klog.InfoLevel, DispatchLogTag, "service %s goroutine done", that.name)
		}
	}(subCtx)

	that.status = idl.ServiceStatusRunning //设置服务状态为运行状态

	<-that.ctx.Context().Done() // 阻塞等待上下文取消
	workerWg.Wait()             // 等待子协程排空数据, 此时 Start 函数会停在这里，直到 runWorker 彻底完成排水

	if that.timer != nil {
		that.timer.Stop()
	}

	if that.logf != nil {
		that.logf(klog.InfoLevel, DispatchLogTag, "service %s done", that.name)
	}
	subCtx.Cancel()
	subCtx.Remove()

	that.onExit(nil)

	return nil
}

func (that *DispatchService) Restart() error {
	if that.status != idl.ServiceStatusRunning { //检查服务状态 是否为运行状态
		err := that.Stop()
		if err != nil {
			return err
		}
	}

	err := that.Start()
	return err
}

func (that *DispatchService) Stop() error {
	if that.draining.Swap(true) {
		that.wg.Wait()
		return nil
	}

	that.queue.Close()
	that.ctx.Cancel()

	that.wg.Wait()                         // 只需要等待 Start 函数返回即可, 因为 Start 会在内部等待 workerWg 完成排水，所以这里是安全的
	that.status = idl.ServiceStatusStopped // 设置服务状态为停止状态

	if that.logf != nil {
		that.logf(klog.InfoLevel, DispatchLogTag, "service %s is stopped", that.name)
	}

	return nil
}

func (that *DispatchService) onError(obj any, err error) {
}

func (that *DispatchService) onExit(obj any) {
}

func (that *DispatchService) Status() idl.ServiceStatus {
	return that.status
}

////////////////////////////////////////////////////////////

func (that *DispatchService) DoSendMessages(msgs []*GenericMessage) (bool, error) {
	// 如果正在排水，拒绝接收新数据
	if that.draining.Load() {
		return false, idl.ErrSrvDraining
	}
	if that != nil && that.queue != nil {
		n := that.queue.EnqueueBatch(msgs)
		if n > 0 { // 成功入队
			return true, nil
		} else {
			return false, idl.ErrSrvDraining
		}
	}
	return false, nil
}

func (that *DispatchService) DoSend(msg *GenericMessage) (bool, error) {
	// 如果正在排水，拒绝接收新数据
	if that.draining.Load() {
		return false, idl.ErrSrvDraining
	}
	if that != nil && that.queue != nil {
		status := that.queue.Enqueue(msg)
		if status { // 成功入队
			return true, nil
		} else {
			return false, idl.ErrSrvDraining
		}
	}
	return false, nil
}

////////////////////////////////////////////////////////////

func (that *DispatchService) sendArray(msgs []*GenericMessage) {
	for _, msg := range msgs {
		that.publish(msg.Topic, msg.Message, msg.Properties)
	}
}

func (that *DispatchService) send(msg *GenericMessage) {
	var msgStr string
	if that.dumpHex {
		msgStr = hex.EncodeToString(msg.Message)
	} else {
		msgStr = string(msg.Message)
	}
	if !that.publish(msg.Topic, msg.Message, msg.Properties) {
		if that.logf != nil {

			that.logf(klog.ErrorLevel, DispatchLogTag, "service %s send fault, topic: %s, message: %s", that.name, msg.Topic, msgStr)
		}
	} else {
		if that.logf != nil {
			that.logf(klog.DebugLevel, DispatchLogTag, "service %s sent topic: %s, message: %s", that.name, msg.Topic, msgStr)
		}
	}
}

////////////////////////////////////////////////////////////

func (that *DispatchService) publish(topic string, message []byte, properties map[string]string) bool {
	flag := false
	for _, mqTarget := range that.mqTargets {
		switch mtCtl := mqTarget.(type) {
		case *target.KafkaMQ:
			// 发送数据到KafkaMQ
			flag = mtCtl.Publish(topic, message, properties)
		case *target.RocketMQ:
			flag = mtCtl.Publish(topic, message, properties)
		case *target.MqttMQ:
			flag = mtCtl.Publish(topic, message, properties)
		case *target.RedisMQ:
			flag = mtCtl.PublishMessage(topic, string(message))
		case *target.RabbitMQ:
			flag = mtCtl.Publish(topic, message, properties)
		case *target.NatsCoreMQ:
			flag = mtCtl.Publish(topic, message, properties)
		case *target.NatsJetStreamMQ:
			flag = mtCtl.Publish(topic, message, properties)
		default:

		}
	}

	return flag
}

////////////////////////////////////////////////////////////
