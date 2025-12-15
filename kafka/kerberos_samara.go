package kafka

import (
	"os"
	"strings"

	"github.com/IBM/sarama"
	"github.com/khan-lau/kutils/container/kstrings"
)

// 使用项目内置 Kerberos 文件，不依赖环境变量：
// krb5.conf:    kerberos/krb5.conf
// keytab:       kerberos/jt_zhxny.keytab
// principal:    jt_zhxny@TDH
// serviceName:  kafka

// KerberosOptions 保存 Kerberos 相关配置
// 若不通过环境变量，也可在其他位置构造 KerberosOptions 并调用 ApplyKerberos
// 但为尽量将变更集中于单文件，默认从环境变量读取
type KerberosOptions struct {
	Krb5ConfPath    string
	KeytabPath      string
	Principal       string
	ServiceName     string
	DisablePAFXFAST bool
}

func parsePrincipal(principal string) (username, realm string, err error) {
	parts := strings.Split(principal, "@")
	if len(parts) != 2 {
		return "", "", kstrings.Errorf("invalid principal: {}", principal)
	}
	return parts[0], parts[1], nil
}

// ApplyKerberos 将 Kerberos 相关参数设置到 sarama.Config 上
func ApplyKerberos(cfg *sarama.Config, k KerberosOptions) error {
	if k.Principal == "" || k.Krb5ConfPath == "" || k.KeytabPath == "" {
		return kstrings.Errorf("kerberos options incomplete")
	}

	username, realm, err := parsePrincipal(k.Principal)
	if err != nil {
		return err
	}

	if k.ServiceName == "" {
		k.ServiceName = "kafka"
	}

	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Handshake = true
	cfg.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI

	g := &cfg.Net.SASL.GSSAPI
	g.AuthType = sarama.KRB5_KEYTAB_AUTH
	g.Username = username
	g.Realm = realm
	g.ServiceName = k.ServiceName
	g.KerberosConfigPath = k.Krb5ConfPath
	g.KeyTabPath = k.KeytabPath
	g.DisablePAFXFAST = k.DisablePAFXFAST

	return nil
}

// ValidateKerberosOptions 验证 Kerberos 配置选项是否有效
// 检查：
// 1. 必要参数是否为空
// 2. krb5.conf 和 keytab 文件是否存在
// 3. 文件是否可读
func ValidateKerberosOptions(k KerberosOptions) error {
	// 检查必要参数
	if k.Principal == "" || k.Krb5ConfPath == "" || k.KeytabPath == "" {
		return kstrings.Errorf("kerberos options incomplete: principal={}, krb5ConfPath={}, keytabPath={}", k.Principal, k.Krb5ConfPath, k.KeytabPath)
	}

	// 验证 principal 格式
	if _, _, err := parsePrincipal(k.Principal); err != nil {
		return kstrings.Errorf("invalid principal format: {}", err)
	}

	// 检查 krb5.conf 文件
	if _, err := os.Stat(k.Krb5ConfPath); err != nil {
		if os.IsNotExist(err) {
			return kstrings.Errorf("krb5.conf file not found: {}", k.Krb5ConfPath)
		}
		return kstrings.Errorf("cannot access krb5.conf file: {}", err)
	}

	// 检查 keytab 文件
	if _, err := os.Stat(k.KeytabPath); err != nil {
		if os.IsNotExist(err) {
			return kstrings.Errorf("keytab file not found: {}", k.KeytabPath)
		}
		return kstrings.Errorf("cannot access keytab file: {}", err)
	}

	return nil
}

// applyKerberosProjectFiles 使用项目内置 kerberos 文件启用 Kerberos 认证，不依赖环境变量
func applyKerberosProjectFiles(cfg *sarama.Config) error {
	if cfg == nil {
		return kstrings.Errorf("sarama.Config is nil")
	}
	opts := KerberosOptions{
		Krb5ConfPath:    "kerberos/krb5.conf",
		KeytabPath:      "kerberos/jt_zhxny.keytab",
		Principal:       "jt_zhxny@TDH",
		ServiceName:     "kafka",
		DisablePAFXFAST: false,
	}

	// 先验证配置
	if err := ValidateKerberosOptions(opts); err != nil {
		return kstrings.Errorf("kerberos configuration validation failed: {}", err)
	}

	// 再应用配置
	return ApplyKerberos(cfg, opts)
}

// applyKerberosEnv 依赖环境变量
func applyKerberosEnv(cfg *sarama.Config) error {
	if cfg == nil {
		return kstrings.Errorf("sarama.Config is nil")
	}

	krb5ConfPath := os.Getenv("KRB5_CONF")
	keytabPath := os.Getenv("KRB5_KEYTAB")
	principal := os.Getenv("KRB5_PRINCIPAL")
	serviceName := os.Getenv("KRB5_SERVICE")
	disablePAFXFASTStr := os.Getenv("KRB5_DISABLEPAFXFAST")

	// 如果某些环境变量未设置，则不启用 Kerberos 认证
	if krb5ConfPath == "" || keytabPath == "" || principal == "" {
		return nil
	}

	disablePAFXFAST := disablePAFXFASTStr == "true" || disablePAFXFASTStr == "True" || disablePAFXFASTStr == "TRUE" ||
		disablePAFXFASTStr == "1" ||
		disablePAFXFASTStr == "yes" || disablePAFXFASTStr == "Yes" || disablePAFXFASTStr == "YES"

	opts := KerberosOptions{
		Krb5ConfPath:    krb5ConfPath,
		KeytabPath:      keytabPath,
		Principal:       principal,
		ServiceName:     serviceName,
		DisablePAFXFAST: disablePAFXFAST,
	}

	// 先验证配置
	if err := ValidateKerberosOptions(opts); err != nil {
		return kstrings.Errorf("kerberos configuration validation failed: {}", err)
	}

	// 再应用配置
	return ApplyKerberos(cfg, opts)
}

func UNUSED_() {
	_ = applyKerberosProjectFiles(nil)
}
