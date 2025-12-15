# KMQ

mq 客户端封装, 支持4种MQ
1. RocketMQ
2. RabbitMQ
3. RedisMQ
4. Kafka
5. MQTT - 支持mqtt 3.1 和 mqtt 3.1.1协议, mqtt5不支持
6. NATS - 支持NATS Core 和 部分支持 NATS JetStream模式的push模式, 基于nats server 2.11.7进行测试

使用方法见 `example`目录
1. `source` - comsumer 范例
2. `target` - producer 范例

> rabbitMQ 暂时只支持topic模式

## kafka kerberos 认证配置
kafka 支持kerberOS 认证, 需要配置环境变量
- `KRB5_CONF` krb5.conf文件路径, krb5.conf 是 Kerberos 的核心配置文件，包含 realm、KDC 地址、域名映射、加密类型等信息。如果不设置，默认在系统路径（如 /etc/krb5.conf）查找。
- `KRB5_KEYTAB` keytab文件路径, keytab 文件存储了 principal 的长期密钥
- `KRB5_PRINCIPAL` principal 名称, Kerberos 认证系统中用来唯一标识一个“身份”的名称
- `KRB5_SERVICE` 服务名称, 如kafka, zookeeper等
- `KRB5_DISABLEPAFXFAST` 禁用PAFXFAST, 默认为false, PAFXFAST 是 Kerberos 的一个扩展，用于提供更强的保护