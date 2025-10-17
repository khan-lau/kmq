package dispatch

import (
	"encoding/hex"
	"sync"
	"time"

	"github.com/khan-lau/kmq/example/service/idl"
	"github.com/khan-lau/kmq/example/service/mq/target"
	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/container/kstrings"
	klog "github.com/khan-lau/kutils/klogger"
)

const (
	dispatch_tag = "dispatch_service"
)

type GenericMessage struct {
	Topic      string            `json:"topic"`
	Message    []byte            `json:"message"`
	Properties map[string]string `json:"properties"`
}

// 告警转发到下游MQ

type DispatchService struct {
	ctx    *kcontext.ContextNode
	name   string            // 服务名称
	status idl.ServiceStatus // 服务状态

	msgChan      chan *GenericMessage // 消息通道
	timer        *time.Timer          // 定时器
	sendInterval uint                 // 发送间隔，毫秒
	dumpHex      bool                 // 是否以十六进制形式打印消息内容

	mqTargets map[string]idl.ServiceInterface // 目标MQ服务列表

	mutex  sync.Mutex        // 互斥锁，用于保护共享资源
	buffer []*GenericMessage // 缓冲区，用于存储待发送的消息

	logf klog.AppLogFuncWithTag // 日志函数
}

// NewDispatchService 创建一个新的 DispatchService 实例
//
// 参数:
//
//	ctx: 上下文节点，用于创建子上下文
//	dumpHex: 是否以十六进制形式打印消息内容
//	sendInterval: 消息发送间隔，单位毫秒
//	maxBatchSize: 批量发送时, 单次消息最大条数, 小于等于1时, 不启用批量发送
//	name: 服务名称
//	mqTargets: 目标 MQ 服务列表，用于转发消息到不同的队列或主题
//	logf: 日志记录函数，带有标签的 AppLogFuncWithTag 类型
//
// 返回值:
//
//	*DispatchService: 指向新创建的 DispatchService 实例的指针
func NewDispatchService(ctx *kcontext.ContextNode, dumpHex bool, sendInterval uint, maxBatchSize uint, name string, mqTargets map[string]idl.ServiceInterface, logf klog.AppLogFuncWithTag) *DispatchService {
	var timer *time.Timer
	if maxBatchSize > 1 {
		timer = time.NewTimer(time.Duration(sendInterval) * time.Millisecond)
	}

	subCtx := ctx.NewChild(name)
	service := &DispatchService{
		ctx:          subCtx,
		name:         name,
		status:       idl.ServiceStatusStopped,
		msgChan:      make(chan *GenericMessage, 20000),        // 消息通道
		timer:        timer,                                    // 定时器，用于触发消息发送, 毫秒
		sendInterval: sendInterval,                             // 发送间隔，毫秒
		dumpHex:      dumpHex,                                  // 是否以十六进制形式打印消息内容
		mqTargets:    mqTargets,                                // 目标MQ服务列表，用于转发消息到不同的队列或主题
		buffer:       make([]*GenericMessage, 0, maxBatchSize), // 缓冲区，用于存储待发送的消息
		logf:         logf,
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
				that.logf(klog.ErrorLevel, dispatch_tag, "start service {} error: {}", that.name, err)
			}
			that.onError(that.name, err)
		}

		if that.logf != nil {
			that.logf(klog.DebugLevel, dispatch_tag, "service {} start async", that.name)
		}

		if that.logf != nil {
			that.logf(klog.DebugLevel, dispatch_tag, "service {} start async done", that.name)
		}
	}()
}

func (that *DispatchService) Start() error {
	if that.status != idl.ServiceStatusStopped { //检查服务状态 是否为停止状态
		return kstrings.Errorf("service {} is not stopped, status={}", that.name, that.status)
	}

	if len(that.mqTargets) == 0 {
		return kstrings.Errorf("service {} mqTargets is empty", that.name)
	} else {
		if that.logf != nil {
			that.logf(klog.DebugLevel, dispatch_tag, "service {} mqTargets: {}", that.name, len(that.mqTargets))
		}
	}

	subCtx := that.ctx.NewChild(dispatch_tag + "_start")
	go func(ctx *kcontext.ContextNode) {
		// 创建一个 channel 变量来持有 timer.C, 防止timer 为 nil时导致崩溃
		var timerCh <-chan time.Time
		if that.timer != nil {
			timerCh = that.timer.C
		}

		time.Sleep(time.Duration(5000) * time.Millisecond)

	END_LOOP:
		for {
			select {
			case <-ctx.Context().Done():
				break END_LOOP
			case msg := <-that.msgChan:
				// 处理消息
				// if that.logf != nil {
				// 	that.logf(klog.DebugLevel, dispatch_tag, "service {} transform topic: {} message: {}", that.name, msg.Topic, string(msg.Message))
				// }

				tmpRatio := cap(that.buffer) / 2
				if tmpRatio < 1 {
					tmpRatio = 1
				}

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
			}

		}
		if that.logf != nil {
			that.logf(klog.InfoLevel, dispatch_tag, "service {} goroutine done", that.name)
		}
	}(subCtx)

	that.status = idl.ServiceStatusRunning //设置服务状态为运行状态

	<-that.ctx.Context().Done() // 阻塞等待上下文取消
	subCtx.Cancel()
	subCtx.Remove()

	that.onExit(nil)

	if that.logf != nil {
		that.logf(klog.InfoLevel, dispatch_tag, "service {} done", that.name)
	}
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
	that.ctx.Cancel()                      // 取消上下文
	that.status = idl.ServiceStatusStopped // 设置服务状态为停止状态
	if that.logf != nil {
		that.logf(klog.InfoLevel, dispatch_tag, "service {} is stopped", that.name)
	}
	time.Sleep(500 * time.Millisecond)
	return nil
}

func (that *DispatchService) onError(obj interface{}, err error) {
}

func (that *DispatchService) onExit(obj interface{}) {
}

////////////////////////////////////////////////////////////

func (that *DispatchService) DoSend(msg *GenericMessage) bool {
	if that != nil && that.msgChan != nil {
		select {
		case that.msgChan <- msg:
			return true
		default:
		}
	}
	return false
}

////////////////////////////////////////////////////////////

func (that *DispatchService) sendArray(msgs []*GenericMessage) {
	for _, msg := range msgs {
		that.publish(msg.Topic, msg.Message, nil)
	}
}

func (that *DispatchService) send(msg *GenericMessage) {
	var msgStr string
	if that.dumpHex {
		msgStr = hex.EncodeToString(msg.Message)
	} else {
		msgStr = string(msg.Message)
	}
	if !that.publish(msg.Topic, msg.Message, nil) {
		if that.logf != nil {

			that.logf(klog.ErrorLevel, dispatch_tag, "service {} send fault, topic: {}, message: {}", that.name, msg.Topic, msgStr)
		}
	} else {
		if that.logf != nil {
			that.logf(klog.DebugLevel, dispatch_tag, "service {} sent topic: {}, message: {} ", that.name, msg.Topic, msgStr)
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
