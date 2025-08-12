# KMQ

mq 客户端封装, 支持4种MQ
1. RocketMQ
2. RabbitMQ
3. RedisMQ
4. Kafka
5. MQTT - 支持mqtt 3.1 和 mqtt 3.1.1协议, mqtt5不支持
6. NATS - 支持NATS Core 和 NATS JetStream模式, 基于nats server 2.11.7进行测试

使用方法见 `example`目录
1. `source` - comsumer 范例
2. `target` - producer 范例

> rabbitMQ 暂时只支持topic模式
