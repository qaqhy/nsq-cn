package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/BurntSushi/toml"
	"github.com/judwhite/go-svc"
	"github.com/mreiferson/go-options"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/version"
	"github.com/nsqio/nsq/nsqlookupd"
)

func nsqlookupdFlagSet(opts *nsqlookupd.Options) *flag.FlagSet {
	flagSet := flag.NewFlagSet("nsqlookupd", flag.ExitOnError)

	flagSet.String("config", "", "path to config file")
	flagSet.Bool("version", false, "print version string")

	logLevel := opts.LogLevel
	flagSet.Var(&logLevel, "log-level", "set log verbosity: debug, info, warn, error, or fatal")
	flagSet.String("log-prefix", "[nsqlookupd] ", "log message prefix")
	flagSet.Bool("verbose", false, "[deprecated] has no effect, use --log-level")

	flagSet.String("tcp-address", opts.TCPAddress, "<addr>:<port> to listen on for TCP clients")
	flagSet.String("http-address", opts.HTTPAddress, "<addr>:<port> to listen on for HTTP clients")
	flagSet.String("broadcast-address", opts.BroadcastAddress, "address of this lookupd node, (default to the OS hostname)")

	flagSet.Duration("inactive-producer-timeout", opts.InactiveProducerTimeout, "duration of time a producer will remain in the active list since its last ping")
	flagSet.Duration("tombstone-lifetime", opts.TombstoneLifetime, "duration of time a producer will remain tombstoned if registration remains")

	return flagSet
}

type program struct {
	once       sync.Once
	nsqlookupd *nsqlookupd.NSQLookupd
}

func main() {
	prg := &program{}
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		logFatal("%s", err)
	}
}

func (p *program) Init(env svc.Environment) error {
	if env.IsWindowsService() { // 如果是windows系统则切换到当前目录下
		dir := filepath.Dir(os.Args[0])
		return os.Chdir(dir)
	}
	return nil
}

func (p *program) Start() error {
	opts := nsqlookupd.NewOptions() // 初始化nsqlookupd.Options结构体到opts对象中

	flagSet := nsqlookupdFlagSet(opts) // 根据nsqlookupd.Options结构体默认值初始化flag.FlagSet结构体到flagSet对象中
	flagSet.Parse(os.Args[1:])         // 解析命令行数据到flagSet对象中

	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) { // 命令行检查是否需要输出版本号，是则输出版本号并退出流程
		fmt.Println(version.String("nsqlookupd"))
		os.Exit(0)
	}

	var cfg config
	configFile := flagSet.Lookup("config").Value.String() // 读取命令行中的config配置文件路径
	if configFile != "" {                                 // 存在配置文件路径则将文件内容解析到cfg对象中
		_, err := toml.DecodeFile(configFile, &cfg)
		if err != nil {
			logFatal("failed to load config file %s - %s", configFile, err)
		}
	}
	cfg.Validate() // 验证文件内容是否存在逻辑漏洞，根据配置文件更新cfg对象部分成员值数据

	options.Resolve(opts, flagSet, cfg) // 解析flagSet和cfg对象，将数据更新到opts对象中

	nsqlookupd, err := nsqlookupd.New(opts) // 根据opts对象创建nsqlookupd对象
	if err != nil {
		logFatal("failed to instantiate nsqlookupd", err)
	}
	p.nsqlookupd = nsqlookupd

	go func() { // 协程方式启动nsqlookupd主程序服务
		err := p.nsqlookupd.Main() // 启动主程序服务并等待结果输出
		if err != nil {            // 结果为error对象时则执行暂停回收逻辑，并发送异常结束code
			p.Stop()
			os.Exit(1)
		}
	}()

	return nil
}

func (p *program) Stop() error {
	p.once.Do(func() {
		p.nsqlookupd.Exit() // nsqlookupd对象优雅退出服务
	})
	return nil
}

func logFatal(f string, args ...interface{}) {
	lg.LogFatal("[nsqlookupd] ", f, args...)
}
