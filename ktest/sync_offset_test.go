package ktest

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/khan-lau/kmq/example/bean/mq"
	"github.com/khan-lau/kutils/filesystem"
	klog "github.com/khan-lau/kutils/klogger"
)

var ( // 全局变量
	glog *klog.Logger // 日志
)

func init() {
	glog = klog.LoggerInstanceOnlyConsole(int8(klog.DebugLevel))
}

func LogFunc(lvl klog.Level, tag, f string, args ...interface{}) {
	if nil == glog {
		glog = klog.LoggerInstanceOnlyConsole(int8(klog.DebugLevel))
		glog.Warrn("Not init logger")
	}

	skip := 1
	switch tag {
	case "RABBIT":
		skip = 2
	}

	switch lvl {
	case klog.DebugLevel:
		if tag == "kafka" {

		} else {
			glog.KD(skip, f, args...)
		}
	case klog.InfoLevel:
		glog.KI(skip, f, args...)
	case klog.WarnLevel:
		glog.KW(skip, f, args...)
	case klog.ErrorLevel:
		glog.KE(skip, f, args...)
	case klog.DPanicLevel:
		glog.KD(skip, f, args...)
	case klog.FatalLevel:
		glog.KF(skip, f, args...)
	default:
		glog.KI(skip, lvl.String()+": "+f, args)
	}
}

func TestOffsetSync(t *testing.T) {
	syncFile := "offset.json"
	offsetSync := mq.NewOffsetSync(0, syncFile, LogFunc)
	offsetSync.SyncTime = 0
	offsetSync.Set("kafkamq", "test_topic", "0", 1234567890)
	offsetSync.Set("kafkamq", "test_topic", "1", 4324)
	offsetSync.Set("kafkamq", "test_topic", "2", 12313)

	t.Logf("offset json: %s\n", offsetSync.ToJson())

	offsetSync.Sync(false)
}

func TestOffsetLoad(t *testing.T) {
	syncFile := "offset.json"
	offsetSync := mq.NewOffsetSync(0, syncFile, LogFunc)
	offsetSync.SyncTime = 0
	// 载入本地缓存文件
	if filesystem.IsFileExists(syncFile) {
		if buf, err := os.ReadFile(syncFile); err != nil {
			glog.E("read file {} error: {}", syncFile, err)
		} else {
			tmap := make(map[string]map[string]map[string]int64)
			if err = json.Unmarshal(buf, &tmap); err != nil {
				glog.E("unmarshal file {} error: {}", syncFile, err)
			} else {
				catcheOffset := offsetSync.Records
				for mqType, topicMap := range tmap {
					catcheOffset[mqType] = make(map[string]map[string]int64)
					for topic, partitionOffset := range topicMap {
						catcheOffset[mqType][topic] = make(map[string]int64)
						for part, offset := range partitionOffset {
							glog.I("load offset: mqType:{} topic:{} partition{}: offset:{}", mqType, topic, part, offset)
							catcheOffset[mqType][topic][part] = offset
						}
					}
				}
			}
		}
	}

	t.Logf("offset json: %s\n", offsetSync.ToJson())
}
