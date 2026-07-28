// Package router 提供消息路由和转发的核心管道服务。
package router

import (
	"fmt"
	"strings"

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
//  2. 对每个目标名称，直接将整批 msgs 的引用放入返回 map。
//  3. Pipeline 的 send() 方法遍历返回的 map，将消息投递到对应目标。
//
// 参数:
//   - ctx: 上下文节点（当前版本未使用，保留接口兼容性）。
//   - msgs: Pipeline 缓冲区提取的一批消息。
//
// 返回值:
//   - map[string][]GenericMessage: 目标名称 → 消息列表 的映射。
//     所有 value 共享同一个 msgs 底层数组（零拷贝），
//     Pipeline 在本次发送完成前保证 msgs 不被修改。
//   - bool: 是否要求pipeline打印调试信息
func (that *BroadcastProcessor) Process(_ *kcontext.ContextNode, msgs []GenericMessage) (map[string][]GenericMessage, bool) {
	result := make(map[string][]GenericMessage, len(that.toTargets))
	for _, name := range that.toTargets {
		result[name] = msgs
	}
	return result, true
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
