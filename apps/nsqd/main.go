package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/judwhite/go-svc"
	"github.com/mreiferson/go-options"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/version"
	"github.com/nsqio/nsq/nsqd"
)

type program struct {
	once sync.Once
	nsqd *nsqd.NSQD
}

func main() {
	prg := &program{}
	if err := svc.Run(prg, syscall.SIGINT, syscall.SIGTERM); err != nil {
		logFatal("%s", err)
	}
}

func (p *program) Init(env svc.Environment) error {
	opts := nsqd.NewOptions() // 初始化nsqd.Options结构体到opts对象中

	flagSet := nsqdFlagSet(opts) // 根据nsqd.Options结构体默认值初始化flag.FlagSet结构体到flagSet对象中
	flagSet.Parse(os.Args[1:])   // 解析命令行数据到flagSet对象中

	rand.Seed(time.Now().UTC().UnixNano()) // 设置随机种子

	if flagSet.Lookup("version").Value.(flag.Getter).Get().(bool) { // 命令行检查是否需要输出版本号，是则输出版本号并退出流程
		fmt.Println(version.String("nsqd"))
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

	nsqd, err := nsqd.New(opts) // 根据opts对象创建nsqd对象
	if err != nil {
		logFatal("failed to instantiate nsqd - %s", err)
	}
	p.nsqd = nsqd

	return nil
}

func (p *program) Start() error {
	err := p.nsqd.LoadMetadata() // 从磁盘文件中加载元数据
	if err != nil {
		logFatal("failed to load metadata - %s", err)
	}
	err = p.nsqd.PersistMetadata() // 持久元数据到磁盘文件中
	if err != nil {
		logFatal("failed to persist metadata - %s", err)
	}

	go func() { // 协程方式启动nsqd主程序服务
		err := p.nsqd.Main() // 启动主程序服务并等待结果输出
		if err != nil {      // 结果为error对象时则执行暂停回收逻辑，并发送异常结束code
			p.Stop()
			os.Exit(1)
		}
	}()

	return nil
}

func (p *program) Stop() error {
	p.once.Do(func() {
		p.nsqd.Exit() // nsqd对象优雅退出服务
	})
	return nil
}

func (p *program) Handle(s os.Signal) error {
	return svc.ErrStop
}

// Context返回在nsqd启动关闭时将被取消的上下文
func (p *program) Context() context.Context {
	return p.nsqd.Context()
}

func logFatal(f string, args ...interface{}) {
	lg.LogFatal("[nsqd] ", f, args...)
}
