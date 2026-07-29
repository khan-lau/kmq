// Package router 提供消息路由和转发的核心管道服务。
package router

import (
	"fmt"
	"strings"
	"sync"

	"github.com/khan-lau/kutils/container/kcontext"
)

////////////////////////////////////////////////////////////

// BroadcastProcessor 实现 Processor 接口的消息广播处理器。
//
// 功能：
//   - 将 Pipeline 输出的一批消息同时分发到所有已指定的下游 MQ 目标。
//   - 采用 "一入多出" 的广播策略，各目标收到的消息内容完全相同。
//   - 自身不维护队列和 goroutine，仅作为 Pipeline 的 Processor 回调被调用。
//
// 使用示例：
//
//	processor := router.NewBroadcastProcessor([]string{"kafkaTarget", "natsTarget"})
//	pipeline := router.NewPipeline(ctx, dumpHex, sendInterval, queueSize, maxBatchSize,
//	    "dispatch", mqTargets, processor, logf)
type BroadcastProcessor struct {
	// toTargets 需要广播的目标名称列表，与 Pipeline 的 mqTargets 中的 key 对应。
	toTargets []string
}

// NewBroadcastProcessor 创建一个 BroadcastProcessor 实例。
//
// 参数:
//   - toTargets: 目标名称列表。列表中的每个名称需与 Pipeline 构造时传入的
//     mqTargets map 中的 key 一致，否则消息发送时会被丢弃。
//
// 返回值:
//   - *BroadcastProcessor: 新创建的广播处理器指针。
func NewBroadcastProcessor(toTargets []string) *BroadcastProcessor {
	return &BroadcastProcessor{toTargets: toTargets}
}

// Process 实现 Processor 接口，将一批消息广播到所有指定的下游 MQ 目标。
//
// 处理逻辑：
//  1. 遍历 toTargets 列表。
//  2. 对每个目标，调用 sender.SendToBatch 投递整批消息。
//
// 参数:
//   - ctx: 上下文节点（当前版本未使用，保留接口兼容性）。
//   - maxBatchSize: Pipeline 配置的最大批量大小，只是建议processor按要求处理, 不限制发送数量
//   - msgs: Pipeline 缓冲区提取的一批消息。
//   - sender: Pipeline 注入的投递接口。
func (that *BroadcastProcessor) Process(_ *kcontext.ContextNode, maxBatchSize uint, msgs []GenericMessage, sender Sender) {
	// 特殊处理：如果只有一个目标，直接发送消息
	if len(that.toTargets) == 1 {
		sender.SendToBatch(that.toTargets[0], msgs)
		return
	}

	// 如果有多个目标，使用多线程并发发送消息
	workgroup := sync.WaitGroup{}
	for _, name := range that.toTargets {
		workgroup.Add(1)
		go func(name string, msgs []GenericMessage) {
			defer workgroup.Done()
			sender.SendToBatch(name, msgs)
		}(name, msgs)
	}
	workgroup.Wait()
}

// String 返回 BroadcastProcessor 的字符串表示，用于日志输出和调试。
//
// 格式示例:
//
//	BroadcastProcessor{targets: [kafkaTarget, natsTarget]}
func (that *BroadcastProcessor) String() string {
	names := make([]string, 0, len(that.toTargets))
	for _, name := range that.toTargets {
		names = append(names, name)
	}
	return fmt.Sprintf("BroadcastProcessor{targets: [%s]}", strings.Join(names, ", "))
}
