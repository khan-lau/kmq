package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/khan-lau/kmq-utils/kafkamq"
	"github.com/khan-lau/kmq-utils/rabbitmq"
	"github.com/khan-lau/kmq/internal/config"
	"github.com/khan-lau/kutils/container/kstrings"
	"github.com/khan-lau/kutils/filesystem"
	klog "github.com/khan-lau/kutils/klogger"
)

const (
	DEFAULT_LOGGER_TAG = "service_manager"
)

func getDefaultConfPath(dir string) string {
	confPath = ""
	if confPath = "conf/conf.json5"; filesystem.IsFileExists(confPath) {
		fmt.Println("Current configure file path: ", filepath.Join(dir, confPath))
	} else if confPath = "conf.json5"; filesystem.IsFileExists(confPath) {
		fmt.Println("Current configure file path: ", filepath.Join(dir, confPath))

	} else if confPath = "conf/conf.json"; filesystem.IsFileExists(confPath) {
		fmt.Println("Current configure file path: ", filepath.Join(dir, confPath))
	} else if confPath = "conf.json"; filesystem.IsFileExists(confPath) {
		fmt.Println("Current configure file path: ", filepath.Join(dir, confPath))

	} else if confPath = "conf/conf.toml"; filesystem.IsFileExists(confPath) {
		fmt.Println("Current configure file path: ", filepath.Join(dir, confPath))
	} else if confPath = "conf.toml"; filesystem.IsFileExists(confPath) {
		fmt.Println("Current configure file path: ", filepath.Join(dir, confPath))

	} else if confPath = "conf/conf.yaml"; filesystem.IsFileExists(confPath) {
		fmt.Println("Current configure file path: ", filepath.Join(dir, confPath))
	} else if confPath = "conf.yaml"; filesystem.IsFileExists(confPath) {
		fmt.Println("Current configure file path: ", filepath.Join(dir, confPath))
	}
	return confPath
}

func initLog(conf *config.Configure) {
	logDir := kstrings.TrimSpace(conf.Log.LogDir)
	if len(logDir) == 0 {
		logDir = "logs"
	}

	if !strings.HasSuffix(logDir, "/") {
		logDir = logDir + "/"
	}

	if os.MkdirAll(logDir, os.ModePerm) != nil {
		fmt.Println("logs dir not created!")
	}

	logLevel := conf.Log.LogLevel

	filename := fmt.Sprintf("%s%s.log", logDir, BuildName)

	logConfig := klog.NewConfigure().SetLogFile(filename).SetLevel(klog.Level(logLevel)).
		SetMaxAge(conf.Log.MaxAge).SetMaxSize(conf.Log.MaxSize).SetRotationTime(conf.Log.RotationTime).
		ShowConsole(conf.Log.Console).
		SetAsync(conf.Log.Async, conf.Log.FlushInterval, conf.Log.BufferSize).
		IsColorful(conf.Log.Colorful)

	// logConfig.SetLogFile("") // 不写入日志文件

	glog = klog.GetLoggerWithConfig(logConfig)

	glog.I("log filename: %s", filename)
}

func showUsage() {
	fmt.Println()
	fmt.Printf("%s %s\nAuthor: %s \nrelease: %s\n\n", BuildName, BuildVersion, BuildPerson, BuildTime)
	if !ver {
		fmt.Printf("Usage: %s -conf <path>\n", BuildName)
		fmt.Println("Options:")
		flag.PrintDefaults()
	}
}

///////////////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////////////

func LogFunc(lvl klog.Level, tag, f string, args ...any) {
	if nil == glog {
		glog = klog.LoggerInstanceOnlyConsole(int8(klog.DebugLevel))
		glog.Warrn("Not init logger")
	}

	skip := 1
	switch tag {
	case rabbitmq.RabbitLogPrefix:
		skip = 2
	}

	switch lvl {
	case klog.DebugLevel:
		if tag == kafkamq.KafkaLogTag {

		} else {
			glog.KDebug(skip, f, args...)
			// glog.KD(skip, f, args...)
		}
	case klog.InfoLevel:
		glog.KInfo(skip, f, args...)
		// glog.KI(skip, f, args...)
	case klog.WarnLevel:
		glog.KWarrn(skip, f, args...)
		// glog.KW(skip, f, args...)
	case klog.ErrorLevel:
		if tag == "kafkamq_target" {

		} else {
			glog.KError(skip, f, args...)
			// glog.KE(skip, f, args...)
		}
	case klog.DPanicLevel:
		glog.KError(skip, f, args...)
		// glog.KDP(skip, f, args...)
	case klog.FatalLevel:
		glog.KFatal(skip, f, args...)
		// glog.KF(skip, f, args...)
	default:
		glog.KInfo(skip, lvl.String()+": "+f, args)
		// glog.KI(skip, lvl.String()+": "+f, args)
	}
}
