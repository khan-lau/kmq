package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/khan-lau/kmq/example/bean/config"
	"github.com/khan-lau/kmq/example/bean/mq"
	"github.com/khan-lau/kmq/example/service/dispatch"
	"github.com/khan-lau/kmq/example/service/idl"
	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/container/klists"
	"github.com/khan-lau/kutils/container/kstrings"
	"github.com/khan-lau/kutils/filesystem"
	klog "github.com/khan-lau/kutils/klogger"
)

var ( // 程序信息
	BuildVersion = "v0.1.0"
	BuildTime    = "2022-09-06 13:58:00"
	BuildName    = "edgeBroker"
	BuildPerson  = "LiuKun"
)

var ( // 命令行参数
	confPath string
	help     bool
	ver      bool
)

var ( // 全局变量
	glog             *klog.Logger                    // 日志
	gMqSourceManager map[string]idl.ServiceInterface // 消息队列来源服务
	gMqTargetManager map[string]idl.ServiceInterface // 消息队列分发服务
	gDispatcher      *dispatch.DispatchService       // 消息分发服务
	gOffsetSync      *mq.OffsetSync                  // topic offset同步服务, 用于记录topic offset, 以便重启时恢复offset

)

// 配置命令行参数
func init() {
	flag.StringVar(&confPath, "conf", "", "configure file path")
	flag.BoolVar(&help, "help", false, "show usage")
	flag.BoolVar(&ver, "version", false, "show version")
}

func main() {
	flag.Parse()

	if ver {
		showUsage()
		os.Exit(0)
	}

	if help {
		showUsage()
		os.Exit(0)
	}

	fmt.Printf("OS: %s\nArchitecture: %s\nRuntime Version: v%s\n\n", runtime.GOOS, runtime.GOARCH, strings.Replace(runtime.Version(), "go", "", 1))
	fmt.Printf("%s Version %s, Build %s\n", BuildName, BuildVersion, BuildTime)

	execPath, _ := filesystem.GetExecPath()
	currentDir := filesystem.GetCurrentDirectory()
	workDir, _ := os.Getwd()

	fmt.Printf("ExecPath: %s\n", execPath)
	fmt.Printf("currentDir: %s\n", currentDir)
	fmt.Printf("workDir: %s\n", workDir)

	// 若未指定配置文件名, 则默认为 "./conf.json5" 或 "./conf/conf.json5"
	if confPath == "" {
		confPath = getDefualtConfPath(workDir)
		if confPath == "" {
			fmt.Fprintf(os.Stderr, "  Error: please specify the path of the configuration file\n")
			showUsage()
			os.Exit(1)
		}
	} else {
		kstrings.Println("Current configure file path: {}", confPath)
	}

	conf, err := config.ConfigInstance(confPath)
	if nil != err {
		fmt.Fprintf(os.Stderr, "Get config file error: %s \nPlease check conf file\n", err.Error())
		return
	}

	initLog(conf)
	glog.I("logger initialized")

	defer func() {
		if r := recover(); r != nil {
			glog.E("Panic recovered: %v", r)
			// 记录堆栈信息
			debug.PrintStack()
		}
	}()

	//  各种资源初始化
	rootContext := kcontext.NewContextTree("rootContext")
	mainCtx := rootContext.GetRoot()

	confDir, _ := filepath.Split(confPath)
	replayDataPath := confDir + "test.message"
	workReplayDataPath := filepath.Join(workDir, confDir, "test.message")
	if conf.SendFile != "" { // 如果指定了重放文件, 则读取重放记录
		workReplayDataPath = conf.SendFile
		replayDataPath = conf.SendFile
	}

	// 如果存在offset记录, 则修改conf中的topic offset
	gOffsetSync = loadOffsetCache(conf)
	syncTimer := time.NewTimer(time.Duration(conf.SyncTime) * time.Millisecond)

	gMqSourceManager = make(map[string]idl.ServiceInterface) // 源MQ服务管理器
	gMqTargetManager = make(map[string]idl.ServiceInterface)

	sigChan := make(chan os.Signal, 1)                    // 创建一个信号通道
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM) // 注册要捕获的信号

	// 监听信号
	go func() {
		sig := <-sigChan // 阻塞等待信号
		glog.I("Get a signal: {} - {}", sig, sig.String())
		mainCtx.Cancel()
	}()

	syncCtx := mainCtx.NewChild("offsetSync")
	waitGroup := &sync.WaitGroup{}

	if conf.Type == "send" {
		// 消息队列target服务
		waitGroup.Add(1)
		go func(ctx *kcontext.ContextNode) {
			defer waitGroup.Done()
			glog.I("start manager mq target service")
			startMqTarget(ctx, conf.Target, LogFunc)
		}(mainCtx)

		//  此处添加一个生产数据的服务
		waitGroup.Add(1)
		go func(ctx *kcontext.ContextNode) {
			defer waitGroup.Done()
			gDispatcher = dispatch.NewDispatchService(ctx, 0, 1, "dispatch", gMqTargetManager, LogFunc)

			var messages *klists.KList[*dispatch.GenericMessage]
			if filesystem.IsFileExists(replayDataPath) { // 如果存在重放文件, 则读取重放记录
				fmt.Printf("founded test file : %s\n", replayDataPath)
				messages = getReplayData(replayDataPath)
			} else if filesystem.IsFileExists(workReplayDataPath) { // 如果存在重放文件, 则读取重放记录
				fmt.Printf("founded test file : %s\n", workReplayDataPath)
				messages = getReplayData(workReplayDataPath)
			} else {
				// fmt.Printf("not found test file : %s or %s \n", replayDataPath, workReplayDataPath)
				glog.E("error: not found replay file : {} or {}", replayDataPath, workReplayDataPath)
				mainCtx.Cancel()
				return
			}

			gDispatcher.StartAsync()

			msgArr := make([]*dispatch.GenericMessage, 0)
			if messages != nil {
				msgArr = klists.ToKSlice(messages)
			}
			sendCtx := mainCtx.NewChild("sendCtx")
			go func(ctx *kcontext.ContextNode, sendInterval uint32, messages []*dispatch.GenericMessage) {
				// 创建一个定时器，每隔`ScanInterval`秒执行一次
				index := 0
				timer := time.NewTimer(time.Duration(sendInterval) * time.Millisecond)
				defer timer.Stop()
			EndScanLoop:
				for {
					select {
					case <-timer.C:
						if len(messages) > 0 {
							if len(messages) > 0 {
								if index >= len(messages) {
									index = 0
								}
								message := messages[index]
								if message.Topic == "quit" && len(message.Message) == 0 {
									timer.Stop()
									err := fmt.Errorf("%s", "Quit send goroutine")
									glog.E("{}", err)
									break EndScanLoop
								}
								index++
								generalMessage(gDispatcher, conf.ResetTimestamp, message)
							}
						} else {
							glog.D("{}", "replay data is empty")
						}

						timer.Reset(time.Duration(sendInterval) * time.Millisecond) // 重置定时器
					case <-ctx.Context().Done(): // 如果 context 被取消，退出循环
						glog.I("{}", "Publisher send goroutine done")
						break EndScanLoop
					}
				}
				glog.I("{}", "Publisher send goroutine finish")

			}(sendCtx, conf.SendInterval, msgArr)
			glog.I("replay data is finished")
		}(mainCtx)
	} else {

		// MQ offset sync服务, 该服务用于定时将当前offset记录到文件中, 以便重启时恢复offset
		waitGroup.Add(1)

		go func(ctx *kcontext.ContextNode) {
			defer waitGroup.Done()
			glog.I("start offset sync service")
			go func(ctx *kcontext.ContextNode) {
			END_LOOP:
				for {
					select {
					case <-syncTimer.C:
						gOffsetSync.Sync()
						syncTimer = time.NewTimer(time.Duration(conf.SyncTime) * time.Millisecond)
						// glog.I("sync offset to file: {}", conf.SyncFile)
					case <-ctx.Context().Done():
						break END_LOOP
					}
				}
				glog.I("offset sync service is finished")
			}(ctx)
		}(syncCtx)

		// 消息队列source服务
		waitGroup.Add(1)
		go func(ctx *kcontext.ContextNode) {
			defer waitGroup.Done()
			glog.I("start manager mq source service")
			startMqSource(ctx, conf.Source, gOffsetSync, LogFunc)
		}(mainCtx)
	}

	waitGroup.Wait()

	<-mainCtx.Context().Done()

	//  销毁资源
	syncCtx.Cancel() // 取消offset sync服务
	syncCtx.Remove()

	stopMqSourceManager() // 停止消息队列source服务
	stopMqTargetManager() // 停止消息队列target服务

	syncTimer.Stop() // 停止定时器
	gOffsetSync.Sync()

	mainCtx.Remove() // 清理主上下文
	glog.I("{} is finished", BuildName)
}
