package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/khan-lau/kmq/example/bean/config"
	"github.com/khan-lau/kutils/container/kstrings"
	"github.com/khan-lau/kutils/filesystem"
	klog "github.com/khan-lau/kutils/klogger"
)

const (
	DEFAULT_LOGGER_TAG = "service_manager"
)

func getDefualtConfPath(dir string) string {
	confPath = ""
	if confPath = "conf/conf.json5"; filesystem.IsFileExists(confPath) {
		kstrings.Println("Current configure file path: {}\n", filepath.Join(dir, confPath))
	} else if confPath = "conf.json5"; filesystem.IsFileExists(confPath) {
		kstrings.Println("Current configure file path: {}\n", filepath.Join(dir, confPath))

	} else if confPath = "conf/conf.json"; filesystem.IsFileExists(confPath) {
		kstrings.Println("Current configure file path: {}\n", filepath.Join(dir, confPath))
	} else if confPath = "conf.json"; filesystem.IsFileExists(confPath) {
		kstrings.Println("Current configure file path: {}\n", filepath.Join(dir, confPath))

	} else if confPath = "conf/conf.toml"; filesystem.IsFileExists(confPath) {
		kstrings.Println("Current configure file path: {}\n", filepath.Join(dir, confPath))
	} else if confPath = "conf.toml"; filesystem.IsFileExists(confPath) {
		kstrings.Println("Current configure file path: {}\n", filepath.Join(dir, confPath))

	} else if confPath = "conf/conf.yaml"; filesystem.IsFileExists(confPath) {
		kstrings.Println("Current configure file path: {}\n", filepath.Join(dir, confPath))
	} else if confPath = "conf.yaml"; filesystem.IsFileExists(confPath) {
		kstrings.Println("Current configure file path: {}\n", filepath.Join(dir, confPath))
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
		kstrings.Println("logs dir not created!")
	}

	logLevel := conf.Log.LogLevel

	filename := kstrings.FormatString("{}{}.log", logDir, BuildName)

	logConfig := klog.NewConfigure().SetLogFile(filename).SetLevel(klog.Level(logLevel)).
		SetMaxAge(conf.Log.MaxAge).SetRotationTime(conf.Log.RotationTime).
		ShowConsole(conf.Log.Console).IsColorful(conf.Log.Colorful)

	// logConfig.SetLogFile("") // 不写入日志文件

	glog = klog.GetLoggerWithConfig(logConfig)

	glog.I("log filename: {}", filename)
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
		glog.KD(skip, f, args...)
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
