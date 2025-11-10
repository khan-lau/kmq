package mq

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	"maps"

	klog "github.com/khan-lau/kutils/klogger"
)

type OffsetSync struct {
	recordMutext sync.Mutex                             // 读写锁,用于读写topic offset
	Records      map[string]map[string]map[string]int64 `json:"records"` // topic offset, "mq类型":{"topic":{"partition or queue":offset}}
	Timestamp    int64                                  `json:"-"`       // 上次同步时间
	SyncTime     uint64                                 `json:"-"`       // 同步周期, 单位毫秒,不低于1000毫秒
	modified     bool                                   `json:"-"`       // 是否修改过
	syncFile     string                                 // 同步文件路径
	logf         klog.AppLogFuncWithTag                 // 日志函数
}

const (
	offset_tag = "offset_service"
)

func NewOffsetSync(syncTime uint64, syncFilePath string, logf klog.AppLogFuncWithTag) *OffsetSync {
	offsetSync := &OffsetSync{
		recordMutext: sync.Mutex{},
		Records:      make(map[string]map[string]map[string]int64),
		SyncTime:     syncTime,
		Timestamp:    time.Now().Unix(),
		modified:     false,
		syncFile:     syncFilePath,
		logf:         logf,
	}
	if syncTime < 1000 {
		offsetSync.SyncTime = 1000
	}
	// go func() {
	// 	// 使用一个 ticker 来周期性地触发同步
	// 	ticker := time.NewTicker(time.Duration(offsetSync.SyncTime) * time.Millisecond)
	// 	for {
	// 		<-ticker.C
	// 		offsetSync.Sync(false)
	// 	}
	// }()
	return offsetSync
}

func (that *OffsetSync) Set(mqType string, topic string, partition string, offset int64) {
	if mqType == "" || mqType == "rabbitmq" || mqType == "redismq" || mqType == "mqtt3" || mqType == "natscoremq" {
		return
	}

	that.recordMutext.Lock()

	mqBean, ok := that.Records[mqType]
	if !ok {
		mqBean = make(map[string]map[string]int64)
		that.Records[mqType] = mqBean
	}

	topicBean, ok := mqBean[topic]
	if !ok {
		topicBean = make(map[string]int64)
		mqBean[topic] = topicBean
	}

	topicBean[partition] = offset

	that.modified = true
	that.recordMutext.Unlock()
	that.Sync(false)
}

func (that *OffsetSync) Sync(force bool) {
	// 如果时间未到, 则不进行同步, 避免频繁读写
	current := time.Now().UnixMilli()
	// 没有修改过, 则不进行同步

	if !force {
		if !that.modified {
			return
		}

		if current-that.Timestamp < int64(that.SyncTime) {
			return
		}
	}

	var syncMap map[string]map[string]map[string]int64
	// 尝试加锁, 如果成功则进行同步, 避免多处同时写
	if that.recordMutext.TryLock() {
		that.Timestamp = current
		tmap := make(map[string]map[string]map[string]int64)

		// 深度拷贝map
		for mqType, topicMap := range that.Records {
			// 第一层: mqType
			newTopicMap := make(map[string]map[string]int64)
			tmap[mqType] = newTopicMap

			for topic, partitionMap := range topicMap {
				// 第二层: topic
				newPartitionMap := make(map[string]int64)
				newTopicMap[topic] = newPartitionMap

				maps.Copy(newPartitionMap, partitionMap)
			}
		}
		that.modified = false
		syncMap = tmap

		// if that.logf != nil {
		// 	that.logf(klog.DebugLevel, offset_tag, "sync offset to {} file: {}", syncMap, that.syncFile)
		// }
		that.recordMutext.Unlock()
	}

	if syncMap != nil {
		// 同步topic offset
		buf, err := json.Marshal(syncMap)
		if err == nil {
			_ = os.WriteFile(that.syncFile, buf, os.ModePerm)
			if that.logf != nil {
				that.logf(klog.DebugLevel, offset_tag, "sync offset to {} file: {}", string(buf), that.syncFile)
			}
		}
	}

}

func (that *OffsetSync) ToJson() string {
	that.recordMutext.Lock()
	defer that.recordMutext.Unlock()
	buf, err := json.Marshal(that.Records)
	if err != nil {
		return ""
	}
	return string(buf)
}
