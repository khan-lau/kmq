package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/khan-lau/kmq/example/bean/config"
	"github.com/khan-lau/kmq/example/bean/mq"
	"github.com/khan-lau/kmq/example/service/idl"
	"github.com/khan-lau/kutils/container/kcontext"
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

	// TODO 各种资源初始化

	rootContext := kcontext.NewContextTree("rootContext")
	mainCtx := rootContext.GetRoot()

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

		//  TODO 此处添加一个生产数据的服务

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

// func main() {
// 	root := kcontext.NewContextTree("kmq")
// 	mainCtx := root.GetRoot()

// 	// 监听ctrl+c信号，优雅退出
// 	sigChan := make(chan os.Signal, 1)                    // 创建一个信号通道
// 	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM) // 注册要捕获的信号

// 	// 监听信号
// 	go func() {
// 		sig := <-sigChan // 阻塞等待信号
// 		glog.I("Get a signal: {} - {}", sig, sig.String())
// 		mainCtx.Cancel()
// 	}()

// 	workgroup := &sync.WaitGroup{}
// 	// 启动MQTT 消费端
// 	workgroup.Add(1)
// 	go func() {
// 		defer workgroup.Done()
// 		// 消费端逻辑
// 		subCtx := mainCtx.NewChild("gSubscriber")
// 		conf := genMqttConfig()
// 		var err error
// 		conf.ClientID = "gSubscriber_01"
// 		gSubscriber, err = source.NewMqttMQ(subCtx, "gSubscriber", conf, LogFunc)
// 		if err != nil {
// 			glog.E("NewMqttMQ Subscriber error: {}", err)
// 			mainCtx.Cancel()
// 			return
// 		}
// 		gSubscriber.SetOnRecivedCallback(func(origin interface{}, name, topic string, partition int, offset int64, properties map[string]string, message []byte) {
// 			glog.I("收到消息: {} - {}", topic, string(message)) // 收到的消息

// 			switch t := origin.(type) {
// 			case *rabbitmq.Message:
// 				t.Ack(false) // false: 只确认当前这条消息; true: 批量确认 DeliveryTag <= current DeliveryTag 的所有消息
// 			case *rocketmq.Message:
// 				t.Ack() // 批量确认
// 			case *nats.NatsMessage:
// 				t.Ack() // 如果想批量确认 需要将 AckPolicy设置为 `AckAllPolicy`
// 			case *kafka.KafkaMessage:
// 				t.Ack() // 确认当前消息seq之前的所有消息
// 			case nil:
// 				// 不支持ack的MQ 直接忽略
// 			default:
// 				// 其他
// 			}
// 		})
// 		gSubscriber.StartAsync()
// 	}()

// 	// 启动MQTT 生产端
// 	workgroup.Add(1)
// 	go func() {
// 		defer workgroup.Done()
// 		// 生产端逻辑
// 		subCtx := mainCtx.NewChild("gPublisher")
// 		conf := genMqttConfig()
// 		conf.ClientID = "gPublisher_01"
// 		conf.Topics = []string{}
// 		var err error
// 		gPublisher, err = target.NewMqttMQ(subCtx, "gPublisher", conf, LogFunc)
// 		if err != nil {
// 			glog.E("NewMqttMQ Publisher error: {}", err)
// 			mainCtx.Cancel()
// 			return
// 		}

// 		gPublisher.StartAsync()

// 		// 定时发送测试消息
// 		subSendCtx := subCtx.NewChild("gPublisher_send")
// 		timer := time.NewTimer(time.Millisecond * 5000)
// 		go func(ctx *kcontext.ContextNode, timer *time.Timer) {
// 			time.Sleep(10 * time.Second)
// 		END_LOOP:
// 			for {
// 				select {
// 				case <-ctx.Context().Done():
// 					break END_LOOP
// 				case <-timer.C:
// 					if gPublisher != nil {
// 						_ = gPublisher.Publish("kmq/test", []byte(kstrings.FormatString("test message at {}", time.Now().Format(datetime.DATETIME_FORMATTER_Mill))), map[string]string{}) // 发送消息
// 					}
// 					timer.Reset(time.Millisecond * 5000)
// 				}
// 			}

// 			glog.D("gPublisher_send exit")
// 		}(subSendCtx, timer)
// 	}()

// 	workgroup.Wait()
// 	<-mainCtx.Context().Done()

// 	glog.I("main exit")
// }
