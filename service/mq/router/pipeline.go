package router

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
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
	PipelineLogTag = "pipeline_service"
)

//////////////////////////////////////////////////////////////

type GenericMessage struct {
	Tag        string            `json:"tag"`
	Topic      string            `json:"topic"`
	Message    []byte            `json:"message"`
	Properties map[string]string `json:"properties"`
}

func (that *GenericMessage) String() string {
	var b strings.Builder
	b.Grow(len(that.Tag) + len(that.Topic) + len(that.Message) + 32)
	b.WriteString("Tag: ")
	b.WriteString(that.Tag)
	b.WriteString(", Topic: ")
	b.WriteString(that.Topic)
	b.WriteString(", Message: ")
	b.Write(that.Message)
	return b.String()
}

func (that *GenericMessage) ToString() string {
	var sb strings.Builder
	sb.Grow(65536)

	_, _ = sb.WriteString("tag:")
	_, _ = sb.WriteString(that.Tag)

	_, _ = sb.WriteString(" topic:")
	_, _ = sb.WriteString(that.Topic)

	_, _ = sb.WriteString(" message:")
	_, _ = sb.WriteString(string(that.Message))

	if that.Properties != nil {
		_, _ = sb.WriteString(" properties:")
		for k, v := range that.Properties {
			_, _ = sb.WriteString(k)
			_, _ = sb.WriteString("=")
			_, _ = sb.WriteString(v)
			sb.WriteString("\n")
		}
	}
	return sb.String()
}

func (that *GenericMessage) ShortString() string {
	return "Tag: " + that.Tag + ", Topic: " + that.Topic + ", Message.len: " + strconv.Itoa(len(that.Message))
}

////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////

// Processor 是用户需要实现的唯一接口
type Processor interface {
	// Process 处理一批消息，通过 sender 投递到目标
	//
	// 参数:
	//  @param ctx: 上下文节点，用于传递取消信号
	//  @param maxBatchSize: 批量发送时, 单次消息最大条数, 只是processor按要求处理
	//  @param msgs: 从缓冲区提取的一批原始消息
	//  @param sender: Pipeline 注入的投递接口，Processor 调用其 SendTo/SendToBatch 发送消息
	Process(ctx *kcontext.ContextNode, maxBatchSize uint, msgs []GenericMessage, sender Sender)
}

////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////

// Sender Pipeline 提供的投递接口，Processor 调用它发送消息
type Sender interface {
	// SendTo 发送单条消息
	SendTo(target string, msg GenericMessage)

	// SendToBatch 批量发送消息
	SendToBatch(target string, msgs []GenericMessage)
}

////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////

type Pipeline struct {
	ctx    *kcontext.ContextNode
	name   string            // 服务名称
	status idl.ServiceStatus // 服务状态

	draining atomic.Bool // 排水状态管理

	queue        *ksync.LockedRingBuffer[GenericMessage] // 消息队列
	queueSize    uint                                    // 消息队列大小
	maxBatchSize uint                                    // 批量发送时, 单次消息最大条数
	timer        *time.Timer                             // 定时器
	sendInterval uint                                    // 发送间隔，毫秒
	dumpHex      bool                                    // 是否以十六进制形式打印消息内容

	mqTargets map[string]idl.ServiceInterface // 目标MQ服务列表
	processor Processor                       // ← 用户注入的加工处理器

	mutex      sync.Mutex       // 互斥锁，用于保护共享资源
	buffer     []GenericMessage // 缓冲区，用于存储待发送的消息
	bufferSwap []GenericMessage // 用于交换的备用缓冲区（核心改造）提前分配好容量，用于零拷贝交换

	logf klog.AppLogFuncWithTag // 日志函数
	wg   sync.WaitGroup         // 等待组，用于等待排水完成
}

// NewPipeline 创建一个新的 Pipeline 实例
//
// 参数:
//
//	@param ctx: 上下文节点，用于创建子上下文
//	@param dumpHex: 是否以十六进制形式打印消息内容
//	@param sendInterval: 消息发送间隔，单位毫秒
//	@param queueSize: 消息队列大小
//	@param packBuffSize: 缓冲的数据包数量, 小于等于1时, 不启用批量发送
//	@param maxBatchSize: 批量发送时, 单次消息最大条数
//	@param name: 服务名称
//	@param mqTargets: 目标 MQ 服务列表，用于转发消息到不同的队列或主题
//	@param processor: 用户注入的加工处理器，用于处理消息
//	@param logf: 日志记录函数，带有标签的 AppLogFuncWithTag 类型
//
// 返回值:
//
//	@returns *Pipeline: 指向新创建的 Pipeline 实例的指针
func NewPipeline(ctx *kcontext.ContextNode, dumpHex bool, sendInterval uint, queueSize uint, packBuffSize uint, maxBatchSize uint,
	name string,
	mqTargets map[string]idl.ServiceInterface,
	processor Processor,
	logf klog.AppLogFuncWithTag,
) *Pipeline {
	var timer *time.Timer
	if packBuffSize > 1 {
		timer = time.NewTimer(time.Duration(sendInterval) * time.Millisecond)
	}

	queue, err := ksync.NewLockedRingBuffer[GenericMessage](uint64(queueSize))
	if err != nil {
		if logf != nil {
			logf(klog.ErrorLevel, PipelineLogTag, 0, "create ring buffer error: %v", err)
		}
		return nil
	}

	subCtx := ctx.NewChild(name)
	service := &Pipeline{
		ctx:          subCtx,
		name:         name,
		status:       idl.ServiceStatusStopped,
		draining:     atomic.Bool{},                           // 排水状态管理
		queue:        queue,                                   // 消息队列
		queueSize:    queueSize,                               // 消息队列大小
		maxBatchSize: maxBatchSize,                            // 批量发送时, 单次消息最大条数
		timer:        timer,                                   // 定时器，用于触发消息发送, 毫秒
		sendInterval: sendInterval,                            // 发送间隔，毫秒
		dumpHex:      dumpHex,                                 // 是否以十六进制形式打印消息内容
		mqTargets:    mqTargets,                               // 目标MQ服务列表，用于转发消息到不同的队列或主题
		processor:    processor,                               // 用户注入的加工处理器，用于处理消息
		buffer:       make([]GenericMessage, 0, packBuffSize), // 缓冲区，用于存储待发送的消息
		bufferSwap:   make([]GenericMessage, 0, packBuffSize), // 用于交换的备用缓冲区（核心改造）提前分配好容量，用于零拷贝交换
		logf:         logf,
		wg:           sync.WaitGroup{},
	}

	return service
}

func (that *Pipeline) Init() {}

func (that *Pipeline) Name() string {
	return that.name
}

func (that *Pipeline) StartAsync() {
	go func() {
		err := that.Start()
		if err != nil {
			that.log(klog.ErrorLevel, "start service %s error: %v", that.name, err)
			that.onError(that.name, err)
		}

		that.log(klog.DebugLevel, "service %s start async done", that.name)
	}()
}

func (that *Pipeline) Start() error {
	if that.status != idl.ServiceStatusStopped { //检查服务状态 是否为停止状态
		return fmt.Errorf("service %s is not stopped, status=%v", that.name, that.status)
	}

	if len(that.mqTargets) == 0 {
		return fmt.Errorf("service %s mqTargets is empty", that.name)
	} else {
		that.log(klog.DebugLevel, "service %s mqTargets: %d", that.name, len(that.mqTargets))
	}

	that.wg.Add(1) // 成员变量 wg 只负责追踪 Start 函数本身的生命周期
	defer that.wg.Done()

	that.draining.Store(false)

	var workerWg sync.WaitGroup // 使用局部 WaitGroup 控制子协程（搬运工）
	workerWg.Add(1)

	subCtx := that.ctx.NewChild(PipelineLogTag + "_start")
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
			case <-timerCh:
				// 处理定时器消息
				var toProcess []GenericMessage
				that.mutex.Lock()
				if len(that.buffer) > 0 { // 检查缓冲区是否为空
					if len(that.buffer) > 0 { // 检查缓冲区是否为空
						that.buffer, that.bufferSwap = that.bufferSwap, that.buffer
						toProcess = that.bufferSwap
						that.bufferSwap = that.bufferSwap[:0] // 清空备用区
					}

				}
				that.mutex.Unlock()
				if len(toProcess) > 0 { // 检查缓冲区是否为空
					that.processor.Process(ctx, that.maxBatchSize, toProcess, that)
				}
				that.timer.Reset(time.Duration(that.sendInterval) * time.Millisecond) // 重置定时器，继续等待下一次触发

			default:
				// 排水模式
				if that.draining.Load() {
					// 清理 buffer 残留
					that.mutex.Lock()
					if len(that.buffer) > 0 {
						lastBuff := make([]GenericMessage, len(that.buffer))
						copy(lastBuff, that.buffer)
						that.buffer = that.buffer[:0]
						that.mutex.Unlock()

						that.processor.Process(ctx, that.maxBatchSize, lastBuff, that)
					} else {
						that.mutex.Unlock()
					}

					drainBuffer := make([]GenericMessage, that.queueSize)
					n := that.queue.DequeueToWait(drainBuffer, 5000*time.Millisecond)
					if n > 0 {
						that.processor.Process(ctx, that.maxBatchSize, drainBuffer[:n], that)
					}

					break END_LOOP
				} else {
					// 正常模式
					if msg, ok, isValid := that.queue.TryDequeue(); ok && isValid {
						idleCount = 0 // 有数据，重置空闲计数

						// // 处理消息
						// that.log(klog.DebugLevel, "service %s transform topic: %s message: %s", that.name, msg.Topic, string(msg.Message))

						tmpRatio := max(cap(that.buffer)/2, 1) // 计算缓冲区的一半，至少为1

						// 批量发送数组
						if tmpRatio > 1 {
							var buffCopy []GenericMessage
							that.mutex.Lock()                 // 加锁
							if len(that.buffer) >= tmpRatio { // 检查缓冲区是否已满
								buffCopy = make([]GenericMessage, len(that.buffer)) // 创建一个新切片
								copy(buffCopy, that.buffer)
								that.buffer = that.buffer[:0] // 清空缓冲区
							}
							that.buffer = append(that.buffer, msg) // 将消息添加到缓冲区
							that.mutex.Unlock()

							if len(buffCopy) > 0 { // 检查缓冲区是否为空
								that.processor.Process(ctx, that.maxBatchSize, buffCopy, that)
							}
						} else {
							// 单条直接发送
							that.processor.Process(ctx, that.maxBatchSize, []GenericMessage{msg}, that)
						}
					} else if !isValid {
						break END_LOOP // 队列已关闭或缓冲区为nil, 则直接返回
					} else {
						// 没拿到数据，开始退避
						idleCount++ // 连续 1000 次没拿到数据（大概经过了微秒级的尝试）
						if idleCount > 1000 {
							time.Sleep(10 * time.Millisecond) // 进入休眠，防止 CPU 空转 100%, 这里设置 10ms 是延迟与功耗的平衡点
						}
					}
				}
			}
		}
		that.log(klog.InfoLevel, "service %s goroutine done", that.name)
	}(subCtx)

	that.status = idl.ServiceStatusRunning //设置服务状态为运行状态

	<-that.ctx.Context().Done() // 阻塞等待上下文取消
	workerWg.Wait()             // 等待子协程排空数据, 此时 Start 函数会停在这里，直到 runWorker 彻底完成排水

	if that.timer != nil {
		that.timer.Stop()
	}

	that.log(klog.InfoLevel, "service %s done", that.name)
	subCtx.Cancel()
	subCtx.Remove()

	that.onExit(nil)

	return nil
}

func (that *Pipeline) Restart() error {
	if that.status == idl.ServiceStatusRunning { //检查服务状态 是否为运行状态
		err := that.Stop()
		if err != nil {
			return err
		}
	}

	err := that.Start()
	return err
}

func (that *Pipeline) Stop() error {
	if that.draining.Swap(true) {
		that.wg.Wait()
		return nil
	}

	that.queue.Close()
	that.ctx.Cancel()

	that.wg.Wait()                         // 只需要等待 Start 函数返回即可, 因为 Start 会在内部等待 workerWg 完成排水，所以这里是安全的
	that.status = idl.ServiceStatusStopped // 设置服务状态为停止状态

	that.log(klog.InfoLevel, "service %s is stopped", that.name)
	return nil
}

func (that *Pipeline) onError(obj any, err error) {
}

func (that *Pipeline) onExit(obj any) {
}

func (that *Pipeline) Status() idl.ServiceStatus {
	return that.status
}

////////////////////////////////////////////////////////////

func (that *Pipeline) DoTrans(msg GenericMessage) (bool, error) {
	if that.draining.Load() {
		return false, idl.ErrSrvDraining
	}
	if that.queue.Enqueue(msg) {
		return true, nil
	}
	return false, idl.ErrSrvDraining
}

func (that *Pipeline) DoTransMessages(msgs []GenericMessage) (bool, error) {
	// 如果正在排水，拒绝接收新数据
	if that.draining.Load() {
		return false, idl.ErrSrvDraining
	}
	if that.queue != nil {
		n := that.queue.EnqueueBatch(msgs)
		if n > 0 { // 成功入队
			return true, nil
		} else {
			return false, idl.ErrSrvDraining
		}
	}
	return false, nil
}

func (that *Pipeline) sendArray(to string, msgs []GenericMessage) {
	for _, msg := range msgs {
		that.send(to, msg)
	}
}

func (that *Pipeline) send(to string, msg GenericMessage) {
	var msgStr string
	if that.dumpHex {
		msgStr = hex.EncodeToString(msg.Message)
	} else {
		msgStr = string(msg.Message)
	}
	if !that.broadcast(to, msg.Message, msg.Properties) {
		that.log(klog.ErrorLevel, "service %s send fault, topic: %s, message: %s", that.name, msg.Topic, msgStr)
	} else {
		that.log(klog.TraceLevel, "service %s sent topic: %s, message: %s", that.name, msg.Topic, msgStr)
	}
}

////////////////////////////////////////////////////////////

// SendTo 发送单条消息到指定目标
func (that *Pipeline) SendTo(target string, msg GenericMessage) {
	that.send(target, msg)
}

// SendToBatch 批量发送消息到指定目标
func (that *Pipeline) SendToBatch(target string, msgs []GenericMessage) {
	that.sendArray(target, msgs)
}

func (that *Pipeline) broadcast(to string, message []byte, properties map[string]string) bool {
	if that.mqTargets == nil {
		that.log(klog.WarnLevel, "publish: mqTargets is nil")
		return false
	}
	flag := false
	mqTarget, ok := that.mqTargets[to]
	if !ok {
		that.log(klog.DebugLevel, "publish: mqTarget not found, to:%s", to)
		return flag
	}

	switch mtCtl := mqTarget.(type) {
	case *target.NatsCoreMQ:
		// 发送数据到NatsCoreMQ
		flag = mtCtl.Broadcast(message, properties)
	case *target.NatsJetStreamMQ:
		// 发送数据到NatsJetStreamMQ
		flag = mtCtl.Broadcast(message, properties)
	case *target.KafkaMQ:
		// 发送数据到KafkaMQ
		flag = mtCtl.Broadcast(message, properties)
	case *target.RocketMQ:
		flag = mtCtl.Broadcast(message, properties)
	case *target.MqttMQ:
		flag = mtCtl.Broadcast(message, properties)
	case *target.RedisMQ:
		flag = mtCtl.Broadcast(message, properties)
	case *target.RabbitMQ:
		flag = mtCtl.Broadcast(message, properties)
	default:

	}

	return flag
}

////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////

// log 日志记录, 会自动添加 PipelineLogTag
//
//go:inline
func (that *Pipeline) log(level klog.Level, format string, args ...any) {
	if that.logf != nil {
		that.logf(level, PipelineLogTag, 1, format, args...)
	}
}
