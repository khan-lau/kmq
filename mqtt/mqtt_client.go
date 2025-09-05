package mqtt

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"strings"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/khan-lau/kutils/container/kcontext"
	"github.com/khan-lau/kutils/container/kstrings"
	"github.com/khan-lau/kutils/filesystem"
	klog "github.com/khan-lau/kutils/klogger"
)

type SubscribeCallback func(voidObj interface{}, msg *MqttMessage)

type MqttSubPub struct {
	ctx    *kcontext.ContextNode
	conf   *Config
	client paho.Client

	connected bool
	mux       sync.RWMutex // Protects connected state and client

	msgChan  chan *MqttMessage
	chanSize uint // 消息通道大小
	logf     klog.AppLogFuncWithTag
}

func NewMQTTClient(ctx *kcontext.ContextNode, chanSize uint, conf *Config, logf klog.AppLogFuncWithTag) (*MqttSubPub, error) {
	if conf.UseTLS() && !filesystem.IsFileExists(conf.CaCertPath()) {
		return nil, kstrings.Errorf("mqtt tls ca cert path: {} not exists", conf.CaCertPath())
	}

	msgChan := make(chan *MqttMessage, chanSize) // 初始化消息通道
	return &MqttSubPub{
		ctx:       ctx,
		conf:      conf,
		client:    nil,
		connected: false,
		msgChan:   msgChan,
		chanSize:  chanSize,
		logf:      logf,
	}, nil
}

func (that *MqttSubPub) Start() {
	go func() {
		flag := that.SyncStart()
		if !flag && that.logf != nil {
			that.logf(klog.ErrorLevel, mqtt_tag, "mqtt.client {} async start fault", that.conf.ClientId())
		}
	}()
}

func (that *MqttSubPub) SyncStart() bool {
	flag := that.connect()
	if !flag {
		if that.logf != nil {
			that.logf(klog.ErrorLevel, mqtt_tag, "mqtt {} connect fault", that.conf.ClientId())
		}
		return false
	}

	<-that.ctx.Context().Done()

	if that.logf != nil {
		that.logf(klog.InfoLevel, mqtt_tag, "mqtt.SyncProducer close")
	}

	return true
}

func (that *MqttSubPub) Subscribe(voidObj interface{}, callback SubscribeCallback) {
	subCtx := that.ctx.NewChild(kstrings.FormatString("{}_async_sub_{}", mqtt_tag, "child"))
	go func(ctx *kcontext.ContextNode) {
	END_LOOP:
		for {
			select {
			case <-ctx.Context().Done():
				break END_LOOP
			default:
				//  此处订阅失败要死循环继续订阅, 直到成功为止
				if that.asyncSubscribe(voidObj, callback) {
					break END_LOOP
				} else {
					if that.logf != nil {
						that.logf(klog.DebugLevel, mqtt_tag, "mqtt {} subscribe fault, retry", that.conf.ClientId())
					}
				}
			}
		}
	}(subCtx)
}

func (that *MqttSubPub) asyncSubscribe(voidObj interface{}, callback SubscribeCallback) bool {
	that.mux.RLock()
	if that.connected { // 未连接, 订阅失败
		that.mux.RUnlock()

		topicFilters := make(map[string]byte)
		topics := that.conf.Topics()
		if len(topics) > 0 {
			for _, topic := range topics {
				topicFilters[topic] = that.conf.Qos()
			}
			token := that.client.SubscribeMultiple(topicFilters, func(client paho.Client, msg paho.Message) {
				if callback != nil {
					callback(voidObj, &MqttMessage{
						Topic:     msg.Topic(),
						Duplicate: msg.Duplicate(),
						Qos:       msg.Qos(),
						Retained:  msg.Retained(),
						Payload:   msg.Payload(), // []byte
					})
				}
			})
			// 阻塞等待订阅完成
			if token.Wait() && token.Error() != nil {
				if that.logf != nil {
					that.logf(klog.ErrorLevel, mqtt_tag, "mqtt {} subscribe fault: {}", that.conf.ClientId(), token.Error())
				}
				return false
			} else {
				if that.logf != nil {
					that.logf(klog.InfoLevel, mqtt_tag, "mqtt {} subscribe topics: {} finished", that.conf.ClientId(), strings.Join(topics, ", "))
				}
			}

		}
		return true
	} else {
		that.mux.RUnlock()
		if that.logf != nil {
			that.logf(klog.WarnLevel, mqtt_tag, "Client is not connected, can't subscribe")
		}
		return false
	}
}

func (that *MqttSubPub) UnSubscribe(topics ...string) bool {
	that.mux.RLock()
	defer that.mux.RUnlock()
	if that.connected && that != nil {
		token := that.client.Unsubscribe(topics...)

		// 阻塞等待订阅完成
		if token.Wait() && token.Error() == nil {
			return true
		} else {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, mqtt_tag, "mqtt {} unsub fault: {}", that.conf.ClientId(), token.Error())
			}
		}
	}
	return false
}

func (that *MqttSubPub) Publish(message *MqttMessage) bool {
	that.mux.RLock()
	defer that.mux.RUnlock()
	if that.connected && that != nil && that.msgChan != nil {
		select {
		case that.msgChan <- message:
			return true
		default:
		}
	}
	return false
}

func (that *MqttSubPub) PublishMessage(topic string, message string) bool {
	msg := &MqttMessage{
		Topic:     topic,
		Duplicate: false,
		Retained:  true,
		Qos:       that.conf.Qos(),
		Payload:   []byte(message),
	}
	return that.Publish(msg)
}

func (that *MqttSubPub) Send(message *MqttMessage) bool {
	return that.SendData(message.Topic, message.Qos, message.Retained, message.Payload)
}

func (that *MqttSubPub) SendData(topic string, qos byte, retained bool, payload []byte) bool {
	that.mux.RLock()
	defer that.mux.RUnlock()
	if that.client != nil && that.connected {
		token := that.client.Publish(topic, qos, retained, payload)
		if token.Wait() && token.Error() != nil {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, mqtt_tag, "mqtt {} send fault: {}", that.conf.ClientId(), token.Error())
			}
			return false
		}
	} else {
		return false
	}
	return true
}

func (that *MqttSubPub) Close() {
	that.ctx.Cancel()
	if that.client != nil {
		that.client.Disconnect(250) // 等待250ms断开连接
	}
}

//////////////////////////////////////////////////////////////////////////

func (that *MqttSubPub) connect() bool {
	if nil == that.client {
		opts := paho.NewClientOptions()

		if that.logf != nil {
			that.logf(klog.DebugLevel, mqtt_tag, "mqtt {} config {}", that.conf.ClientId(), that.conf.String())
		}

		// 动态设置 Broker URL
		brokerURL := ""
		if that.conf.useTLS {
			brokerURL = "ssl://" + that.conf.Broker()
		} else {
			brokerURL = "tcp://" + that.conf.Broker()
		}
		opts.AddBroker(brokerURL)
		opts.SetClientID(that.conf.ClientId())
		opts.SetUsername(that.conf.Username())
		opts.SetPassword(that.conf.Password())
		opts.SetProtocolVersion(uint(that.conf.Version()))
		opts.SetKeepAlive(time.Duration(that.conf.KeepAlive()))
		opts.SetCleanSession(that.conf.CleanSession())

		// 遗嘱设置
		if len(that.conf.Topics()) > 0 {
			opts.SetWill(that.conf.WillTopic(), string(that.conf.WillPayload()), that.conf.Qos(), that.conf.WillRetain())
		}

		opts.SetAutoReconnect(true)
		opts.SetConnectRetry(true)
		opts.SetConnectRetryInterval(time.Second * 5) // 重连等待时间, SetConnectRetry(true)时才生效
		opts.SetConnectTimeout(time.Duration(that.conf.Timeout()))
		opts.SetPingTimeout(time.Duration(that.conf.Timeout()))

		// TLS 配置
		if that.conf.useTLS {
			tlsConfig := &tls.Config{}
			if that.conf.caCertPath != "" {
				caCert, err := os.ReadFile(that.conf.caCertPath)
				if err != nil {
					that.logf(klog.ErrorLevel, mqtt_tag, "Failed to read CA cert: {}", err)
					return false
				}
				certPool := x509.NewCertPool()
				if !certPool.AppendCertsFromPEM(caCert) {
					that.logf(klog.ErrorLevel, mqtt_tag, "Failed to parse CA cert")
					return false
				}
				tlsConfig.RootCAs = certPool
			} else {
				that.logf(klog.ErrorLevel, mqtt_tag, "No CA cert provided")
				return false
			}
			opts.SetTLSConfig(tlsConfig)
		}

		// 连接成功事件, 连接鉴权成功后才可以开始进行后续的指令操作
		opts.SetOnConnectHandler(func(c paho.Client) {
			that.mux.Lock()
			if that.logf != nil {
				that.logf(klog.InfoLevel, mqtt_tag, "mqtt {} {} connect success", that.conf.ClientId(), that.conf.Broker())
			}

			that.connected = true
			that.onAuthed(true)
			that.mux.Unlock()
		})

		// 连接断开事件, 除了重连外的所有操作都暂停
		opts.SetConnectionLostHandler(func(c paho.Client, err error) {
			that.mux.Lock()
			if that.logf != nil {
				that.logf(klog.InfoLevel, mqtt_tag, "mqtt {} {} connect lost", that.conf.ClientId(), that.conf.Broker())
			}

			that.connected = false
			that.onAuthed(false)
			that.mux.Unlock()
		})

		// 重连事件
		opts.SetReconnectingHandler(func(c paho.Client, option *paho.ClientOptions) {
			if that.logf != nil {
				that.logf(klog.DebugLevel, mqtt_tag, "mqtt {} {} reconnecting", that.conf.ClientId(), that.conf.Broker())
			}
		})

		// 全局 MQTT pub 消息处理, subscribe 操作时没有指定明确回调函数的, 都会走这里处理
		opts.SetDefaultPublishHandler(func(client paho.Client, msg paho.Message) {
			if that.logf != nil {
				that.logf(klog.DebugLevel, mqtt_tag, "mqtt {} {} receive message: {}", that.conf.ClientId(), that.conf.Broker(), string(msg.Payload()))
			}
		})

		client := paho.NewClient(opts)
		that.client = client
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			if that.logf != nil {
				that.logf(klog.ErrorLevel, mqtt_tag, "mqtt {} {} connect error: {}", that.conf.ClientId(), that.conf.Broker(), token.Error())
			}
		}
		return true
	}
	return true
}

func (that *MqttSubPub) ReadySend() {
	subCtx := that.ctx.NewChild(kstrings.FormatString("{}_ready_pub_{}", mqtt_tag, "child"))
	go func(ctx *kcontext.ContextNode) {
	END_LOOP:
		for {
			select {
			case <-ctx.Context().Done():
				// that.log(klog.InfoLevel, "mqtt.SyncProducer cancel")
				break END_LOOP
			case msg := <-that.msgChan:
				// that.logf(klog.DebugLevel, "ready to topic: {} send {}", msg.Topic, string(msg.Payload))
				subPubCtx := that.ctx.NewChild(kstrings.FormatString("{}_pub_msg_{}", mqtt_tag, "child"))

				// 失败无限重试, 直到成功为止
			END_SUB_LOOP:
				for {
					select {
					case <-subPubCtx.Context().Done():
						break END_SUB_LOOP
					default:
						if that.Send(msg) {
							break END_SUB_LOOP
						} else {
							if that.logf != nil {
								that.logf(klog.DebugLevel, mqtt_tag, "mqtt {} publish to {} fault, retry", that.conf.ClientId(), msg.Topic)
							}
							time.Sleep(500 * time.Millisecond) // 等待500ms后重试
						}
					}
				}
				subPubCtx.Cancel()
				subPubCtx.Remove()
			}
		}
	}(subCtx)

}

// 全局鉴权回调事件, 连接并鉴权成功或断开都会触发此函数
func (that *MqttSubPub) onAuthed(isOnAuthed bool) {
	if that.conf.OnAuthedCallback() != nil {
		that.conf.OnAuthedCallback()(that, isOnAuthed)
	}
}
