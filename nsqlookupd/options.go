package nsqlookupd

import (
	"log"
	"os"
	"time"

	"github.com/nsqio/nsq/internal/lg"
)

type Options struct {
	LogLevel  lg.LogLevel `flag:"log-level"`  // 日志输出等级，选项（debug, info, warn, error, or fatal），默认info
	LogPrefix string      `flag:"log-prefix"` // 日志信息输出前缀(default "[nsqlookupd] ")
	Logger    Logger

	TCPAddress       string `flag:"tcp-address"`       // 用于侦听TCP客户端（默认值为“0.0.0.0:4160”）
	HTTPAddress      string `flag:"http-address"`      // 用于侦听HTTP客户端（默认值为“0.0.0.0:4161”）
	BroadcastAddress string `flag:"broadcast-address"` // 服务发现的地址;默认为主机名,nsqd创建topic对象后会通过此地址同步所有注册的channel对象

	InactiveProducerTimeout time.Duration `flag:"inactive-producer-timeout"`
	TombstoneLifetime       time.Duration `flag:"tombstone-lifetime"`
}

func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	return &Options{
		LogPrefix:        "[nsqlookupd] ",
		LogLevel:         lg.INFO,
		TCPAddress:       "0.0.0.0:4160",
		HTTPAddress:      "0.0.0.0:4161",
		BroadcastAddress: hostname,

		InactiveProducerTimeout: 300 * time.Second,
		TombstoneLifetime:       45 * time.Second,
	}
}
