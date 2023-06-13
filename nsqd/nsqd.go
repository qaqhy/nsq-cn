package nsqd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/nsq/internal/clusterinfo"
	"github.com/nsqio/nsq/internal/dirlock"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/statsd"
	"github.com/nsqio/nsq/internal/util"
	"github.com/nsqio/nsq/internal/version"
)

const (
	TLSNotRequired = iota
	TLSRequiredExceptHTTP
	TLSRequired
)

type errStore struct {
	err error
}

type NSQD struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	clientIDSequence int64

	sync.RWMutex
	ctx context.Context
	// ctxCancel cancels a context that main() is waiting on
	ctxCancel context.CancelFunc

	opts atomic.Value

	dl        *dirlock.DirLock
	isLoading int32
	isExiting int32
	errValue  atomic.Value
	startTime time.Time

	topicMap map[string]*Topic

	lookupPeers atomic.Value

	tcpServer     *tcpServer
	tcpListener   net.Listener
	httpListener  net.Listener
	httpsListener net.Listener
	tlsConfig     *tls.Config

	poolSize int

	notifyChan           chan interface{}
	optsNotificationChan chan struct{}
	exitChan             chan int
	waitGroup            util.WaitGroupWrapper

	ci *clusterinfo.ClusterInfo
}

func New(opts *Options) (*NSQD, error) {
	var err error

	dataPath := opts.DataPath
	if opts.DataPath == "" { // 检查是否设置磁盘数据存放根路径，若未设置则设置当前可执行文件的上级路径为磁盘数据存放根路径
		cwd, _ := os.Getwd()
		dataPath = cwd
	}
	if opts.Logger == nil { // 未设置日志对象，则初始化日志对象
		opts.Logger = log.New(os.Stderr, opts.LogPrefix, log.Ldate|log.Ltime|log.Lmicroseconds)
	}

	n := &NSQD{
		startTime:            time.Now(),              // nsqd启动时间，用于nsqd运行信息和统计信息展示
		topicMap:             make(map[string]*Topic), // topicMap对象，用于存放所有的topic与channel之间的关系
		exitChan:             make(chan int),          // nsqd服务退出通知通道
		notifyChan:           make(chan interface{}),  // 通知通道，用于topic、channel对象创建和退出信号的通知使用
		optsNotificationChan: make(chan struct{}, 1),  // 配置修改通道，用于修改时热加载配置信息
		dl:                   dirlock.New(dataPath),   // data目录锁
	}
	n.ctx, n.ctxCancel = context.WithCancel(context.Background())
	httpcli := http_api.NewClient(nil, opts.HTTPClientConnectTimeout, opts.HTTPClientRequestTimeout)
	n.ci = clusterinfo.New(n.logf, httpcli) // nsqd仅用于同步topic下注册的所有channel，后续会讲解所使用的地方

	n.lookupPeers.Store([]*lookupPeer{}) // 缓存空的服务发现对象列表，后面lookupLoop协程会更新，用于n.ci对象同步注册信息的参数

	n.swapOpts(opts)             // 缓存opts对象到n.opts对象中
	n.errValue.Store(errStore{}) // 设置空错误结构体，后面用于http协议下的ping命令检查返回

	err = n.dl.Lock() // 上目录锁，这里内部具体方法还未做实现，目前形同虚设
	if err != nil {
		return nil, fmt.Errorf("failed to lock data-path: %v", err)
	}

	if opts.MaxDeflateLevel < 1 || opts.MaxDeflateLevel > 9 { // 控制客户端deflate压缩数据等级范围[1,9]，用于客户端下使用compress/flate包对应的NewWriter方法，代码fw, _ := flate.NewWriter(conn, level)
		return nil, errors.New("--max-deflate-level must be [1,9]")
	}

	if opts.ID < 0 || opts.ID >= 1024 { // 节点ID范围控制[0,1024)
		return nil, errors.New("--node-id must be [0,1024)")
	}

	if opts.TLSClientAuthPolicy != "" && opts.TLSRequired == TLSNotRequired { // tls客户端身份验证策略检查，若存在则修改设置漏掉的参数tls-required
		opts.TLSRequired = TLSRequired
	}

	tlsConfig, err := buildTLSConfig(opts) // 根据opts对象生成tls配置对象
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config - %s", err)
	}
	if tlsConfig == nil && opts.TLSRequired != TLSNotRequired {
		return nil, errors.New("cannot require TLS client connections without TLS key and cert")
	}
	n.tlsConfig = tlsConfig

	for _, v := range opts.E2EProcessingLatencyPercentiles { // 性能评估百分位数范围限制(0.00,1.00]
		if v <= 0 || v > 1 {
			return nil, fmt.Errorf("invalid E2E processing latency percentile: %v", v)
		}
	}

	n.logf(LOG_INFO, version.String("nsqd")) // nsqd版本号输出
	n.logf(LOG_INFO, "ID: %d", opts.ID)      // nsqd节点ID

	n.tcpServer = &tcpServer{nsqd: n}                                                  // 初始化tcpServer对象
	n.tcpListener, err = net.Listen(util.TypeOfAddr(opts.TCPAddress), opts.TCPAddress) // 初始化tcpListener对象
	if err != nil {
		return nil, fmt.Errorf("listen (%s) failed - %s", opts.TCPAddress, err)
	}
	if opts.HTTPAddress != "" { // HTTPAddress存在则初始化httpListener对象
		n.httpListener, err = net.Listen(util.TypeOfAddr(opts.HTTPAddress), opts.HTTPAddress)
		if err != nil {
			return nil, fmt.Errorf("listen (%s) failed - %s", opts.HTTPAddress, err)
		}
	}
	if n.tlsConfig != nil && opts.HTTPSAddress != "" { // tlsConfig和HTTPSAddress 存在则初始化httpsListener对象
		n.httpsListener, err = tls.Listen("tcp", opts.HTTPSAddress, n.tlsConfig)
		if err != nil {
			return nil, fmt.Errorf("listen (%s) failed - %s", opts.HTTPSAddress, err)
		}
	}
	if opts.BroadcastHTTPPort == 0 { // 广播HTTP端口号不存在则将httpListener对应端口号做广播端口号
		tcpAddr, ok := n.RealHTTPAddr().(*net.TCPAddr)
		if ok {
			opts.BroadcastHTTPPort = tcpAddr.Port
		}
	}

	if opts.BroadcastTCPPort == 0 { // 广播TCP端口号不存在则将tcpListener对应端口号做广播端口号
		tcpAddr, ok := n.RealTCPAddr().(*net.TCPAddr)
		if ok {
			opts.BroadcastTCPPort = tcpAddr.Port
		}
	}

	if opts.StatsdPrefix != "" { // 设置统计前缀时则更新统计前缀
		var port string = fmt.Sprint(opts.BroadcastHTTPPort)
		statsdHostKey := statsd.HostKey(net.JoinHostPort(opts.BroadcastAddress, port))
		prefixWithHost := strings.Replace(opts.StatsdPrefix, "%s", statsdHostKey, -1)
		if prefixWithHost[len(prefixWithHost)-1] != '.' {
			prefixWithHost += "."
		}
		opts.StatsdPrefix = prefixWithHost
	}

	return n, nil
}

// getOpts 获取opts对象
func (n *NSQD) getOpts() *Options {
	return n.opts.Load().(*Options)
}

// swapOpts 存储opts对象
func (n *NSQD) swapOpts(opts *Options) {
	n.opts.Store(opts)
}

// triggerOptsNotification 通知lookupLoop协程存在opts对象参数更新
func (n *NSQD) triggerOptsNotification() {
	select {
	case n.optsNotificationChan <- struct{}{}:
	default:
	}
}

// RealTCPAddr 获取nsqd真实tcp的net.Addr对象
func (n *NSQD) RealTCPAddr() net.Addr {
	if n.tcpListener == nil {
		return &net.TCPAddr{}
	}
	return n.tcpListener.Addr()

}

// RealHTTPAddr 获取nsqd真实http的net.Addr对象
func (n *NSQD) RealHTTPAddr() net.Addr {
	if n.httpListener == nil {
		return &net.TCPAddr{}
	}
	return n.httpListener.Addr()
}

// RealHTTPSAddr 获取nsqd真实https的*net.TCPAddr对象
func (n *NSQD) RealHTTPSAddr() *net.TCPAddr {
	if n.httpsListener == nil {
		return &net.TCPAddr{}
	}
	return n.httpsListener.Addr().(*net.TCPAddr)
}

// SetHealth 设置nsqd错误信息
func (n *NSQD) SetHealth(err error) {
	n.errValue.Store(errStore{err: err})
}

// IsHealthy 判断nsqd服务是否健康
func (n *NSQD) IsHealthy() bool {
	return n.GetError() == nil
}

// GetError 获取nsqd错误信息
func (n *NSQD) GetError() error {
	errValue := n.errValue.Load()
	return errValue.(errStore).err
}

// GetHealth 获取nsqd心跳状态
func (n *NSQD) GetHealth() string {
	err := n.GetError()
	if err != nil {
		return fmt.Sprintf("NOK - %s", err)
	}
	return "OK"
}

func (n *NSQD) GetStartTime() time.Time {
	return n.startTime
}

func (n *NSQD) Main() error {
	exitCh := make(chan error) // 初始化错误通知通道
	var once sync.Once
	exitFunc := func(err error) { // 临时错误通知方法，所有的核心服务必须通过此方法包装才能将错误信息通知到错误通道中
		once.Do(func() {
			if err != nil {
				n.logf(LOG_FATAL, "%s", err)
			}
			exitCh <- err
		})
	}

	n.waitGroup.Wrap(func() { // ***重点：协程方式启动nsqd的TCP服务
		exitFunc(protocol.TCPServer(n.tcpListener, n.tcpServer, n.logf))
	})
	if n.httpListener != nil { // 存在HTTP的net.Listener对象时执行下面逻辑
		httpServer := newHTTPServer(n, false, n.getOpts().TLSRequired == TLSRequired)
		n.waitGroup.Wrap(func() { // ***重点：协程方式启动nsqd的HTTP服务
			exitFunc(http_api.Serve(n.httpListener, httpServer, "HTTP", n.logf))
		})
	}
	if n.httpsListener != nil { // 存在HTTPS的net.Listener对象时执行下面逻辑
		httpsServer := newHTTPServer(n, true, true)
		n.waitGroup.Wrap(func() { // ***重点：协程方式启动nsqd的HTTPS服务，服务内容与http一致
			exitFunc(http_api.Serve(n.httpsListener, httpsServer, "HTTPS", n.logf))
		})
	}

	n.waitGroup.Wrap(n.queueScanLoop)    // ***重点：协程方式启动queueScanLoop服务，处理消费中和延迟优先级队列中的消息
	n.waitGroup.Wrap(n.lookupLoop)       // 协程方式启动lookupLoop服务，用于同步所有的topic和channel到每个服务发现lookup服务中。
	if n.getOpts().StatsdAddress != "" { // 存在统计服务时执行下面逻辑
		n.waitGroup.Wrap(n.statsdLoop) // 协程方式启动statsdLoop服务，使用UDP协议周期上传nsqd服务状态信息
	}

	err := <-exitCh // 阻塞等待退出信号，有错误时返回error对象，无错误时返回nil
	return err
}

// Metadata is the collection of persistent information about the current NSQD.
type Metadata struct {
	Topics  []TopicMetadata `json:"topics"`
	Version string          `json:"version"`
}

// TopicMetadata is the collection of persistent information about a topic.
type TopicMetadata struct {
	Name     string            `json:"name"`
	Paused   bool              `json:"paused"`
	Channels []ChannelMetadata `json:"channels"`
}

// ChannelMetadata is the collection of persistent information about a channel.
type ChannelMetadata struct {
	Name   string `json:"name"`
	Paused bool   `json:"paused"`
}

func newMetadataFile(opts *Options) string {
	return path.Join(opts.DataPath, "nsqd.dat")
}

func readOrEmpty(fn string) ([]byte, error) {
	data, err := os.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read metadata from %s - %s", fn, err)
		}
	}
	return data, nil
}

func writeSyncFile(fn string, data []byte) error {
	f, err := os.OpenFile(fn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	f.Close()
	return err
}

// LoadMetadata 加载元数据
func (n *NSQD) LoadMetadata() error {
	atomic.StoreInt32(&n.isLoading, 1)       // 设置nsqd对象为加载状态
	defer atomic.StoreInt32(&n.isLoading, 0) // LoadMetadata方法退出时取消加载状态

	fn := newMetadataFile(n.getOpts()) // 获取nsqd.dat文件地址

	data, err := readOrEmpty(fn) // 获取nsqd.dat文件数据
	if err != nil {
		return err
	}
	if data == nil { // nsqd第一次启动，没有任何元数据
		return nil // fresh start
	}

	var m Metadata
	err = json.Unmarshal(data, &m) // 序列化元数据
	if err != nil {
		return fmt.Errorf("failed to parse metadata in %s - %s", fn, err)
	}

	for _, t := range m.Topics { // 遍历初始化topic
		if !protocol.IsValidTopicName(t.Name) {
			n.logf(LOG_WARN, "skipping creation of invalid topic %s", t.Name)
			continue
		}
		topic := n.GetTopic(t.Name) // ***重点：获取topic对象，此方法下逻辑相对比较复杂，下面会提出来详解
		if t.Paused {
			topic.Pause()
		}
		for _, c := range t.Channels { // 遍历初始化topic对应的所有channel
			if !protocol.IsValidChannelName(c.Name) {
				n.logf(LOG_WARN, "skipping creation of invalid channel %s", c.Name)
				continue
			}
			channel := topic.GetChannel(c.Name) // 获取channel对象，channel不存在则创建同时通知到topic的channelUpdateChan通道中，使topic对应的消息泵messagePump更新需要同步分发消息msg的channel列表
			if c.Paused {
				channel.Pause()
			}
		}
		topic.Start() // 通知此topic对应的消息泵messagePump启动
	}
	return nil
}

// GetMetadata 检索获取NSQ守护进程的当前所有主题topic和通道channel集。如果设置了ephemeral标志，也会返回ephemeral主题topic，尽管这些主题topic没有被保存到磁盘。
func (n *NSQD) GetMetadata(ephemeral bool) *Metadata {
	meta := &Metadata{
		Version: version.Binary,
	}
	for _, topic := range n.topicMap {
		if topic.ephemeral && !ephemeral {
			continue
		}
		topicData := TopicMetadata{
			Name:   topic.name,
			Paused: topic.IsPaused(),
		}
		topic.Lock()
		for _, channel := range topic.channelMap {
			if channel.ephemeral {
				continue
			}
			topicData.Channels = append(topicData.Channels, ChannelMetadata{
				Name:   channel.name,
				Paused: channel.IsPaused(),
			})
		}
		topic.Unlock()
		meta.Topics = append(meta.Topics, topicData)
	}
	return meta
}

// PersistMetadata 持久元数据
func (n *NSQD) PersistMetadata() error {
	// 持久化我们所拥有的主题topic和频道channel信息
	fileName := newMetadataFile(n.getOpts())

	n.logf(LOG_INFO, "NSQ: persisting topic/channel metadata to %s", fileName)

	data, err := json.Marshal(n.GetMetadata(false)) // json序列化元数据结构体，准备持久化到元数据文件中
	if err != nil {
		return err
	}
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	err = writeSyncFile(tmpFileName, data) // 同步到临时元数据文件中
	if err != nil {
		return err
	}
	err = os.Rename(tmpFileName, fileName) // 将临时元数据文件名更新为正式的元数据文件名
	if err != nil {
		return err
	}
	// technically should fsync DataPath here

	return nil
}

func (n *NSQD) Exit() {
	if !atomic.CompareAndSwapInt32(&n.isExiting, 0, 1) {
		// avoid double call
		return
	}
	if n.tcpListener != nil { // tcpListener对象存在则优雅关闭
		n.tcpListener.Close()
	}

	if n.tcpServer != nil { // tcpServer对象存在则优雅关闭
		n.tcpServer.Close()
	}

	if n.httpListener != nil { // httpListener对象存在则优雅关闭
		n.httpListener.Close()
	}

	if n.httpsListener != nil { // httpsListener对象存在则优雅关闭
		n.httpsListener.Close()
	}

	n.Lock()
	err := n.PersistMetadata()
	if err != nil {
		n.logf(LOG_ERROR, "failed to persist metadata - %s", err)
	}
	n.logf(LOG_INFO, "NSQ: closing topics")
	for _, topic := range n.topicMap {
		topic.Close()
	}
	n.Unlock()

	n.logf(LOG_INFO, "NSQ: stopping subsystems")
	close(n.exitChan)  // 通知所有协程nsqd将开始优雅退出
	n.waitGroup.Wait() // 等待所有协程退出
	n.dl.Unlock()
	n.logf(LOG_INFO, "NSQ: bye")
	n.ctxCancel()
}

// GetTopic 执行一个线程安全操作，返回一个指向主题topic对象的指针（可能是新的）。
func (n *NSQD) GetTopic(topicName string) *Topic {
	// 很可能我们已经有了这个主题topic，所以请先尝试读取锁定
	n.RLock()
	t, ok := n.topicMap[topicName]
	n.RUnlock()
	if ok {
		return t
	}

	// 前面读取锁定未拿到指定主题topic，现在写锁定同样尝试获取（防止并发模式下释放读锁的一瞬间加入了此主题topic）
	n.Lock()

	t, ok = n.topicMap[topicName]
	if ok {
		n.Unlock()
		return t
	}
	// 上面绝对的保证了主题topic不存在，同时加入了写锁不可能在操作期间并发创建此主题topic，所以下面进行topic对象的真正创建
	deleteCallback := func(t *Topic) {
		n.DeleteExistingTopic(t.name)
	}
	t = NewTopic(topicName, n, deleteCallback) // ***重点：此方法下逻辑相对比较复杂，下面会提出来详解
	n.topicMap[topicName] = t

	n.Unlock()

	n.logf(LOG_INFO, "TOPIC(%s): created", t.name)
	// 主题已创建完成，但消息泵尚未启动，后面会由topic.Start()方法通过通道方式启动消息泵

	// 如果该主题是在启动时加载元数据时创建的，请不要进行任何进一步的初始化（加载完成后将“启动”该主题）；由于加载配置更新了n.isLoading字段为1，所以这里会提前结束
	if atomic.LoadInt32(&n.isLoading) == 1 {
		return t
	}

	// 如果使用lookupd，请进行阻塞调用以获取通道并立即创建它们，以确保所有通道都接收到已发布的消息；上面创建新的主题topic对象后需要检查服务发现lookupd中是否已注册对应的topic，若注册则同步对应topic下所有的频道channel
	lookupdHTTPAddrs := n.lookupdHTTPAddrs()
	if len(lookupdHTTPAddrs) > 0 {
		// 遍历所有lookupd服务器获取所有频道channel（去重频道channel列表）
		channelNames, err := n.ci.GetLookupdTopicChannels(t.name, lookupdHTTPAddrs)
		if err != nil {
			n.logf(LOG_WARN, "failed to query nsqlookupd for channels to pre-create for topic %s - %s", t.name, err)
		}
		for _, channelName := range channelNames {
			if strings.HasSuffix(channelName, "#ephemeral") { // 带有#ephemeral前缀的频道channel是临时频道
				continue // 不要在没有消费者客户端的情况下创建临时频道
			}
			t.GetChannel(channelName) // 获取channel对象，channel不存在则创建同时通知到topic的channelUpdateChan通道中，使topic对应的消息泵messagePump更新需要同步分发消息msg的channel列表
		}
	} else if len(n.getOpts().NSQLookupdTCPAddresses) > 0 { // 由于lookupdHTTPAddrs列表为空，则检查配置中的NSQLookupdTCPAddresses是否为空，若存在则打印错误日志（没有可用的nsqlookupd服务来查询是否需要为主题topic同步已存在的所有频道channel）
		n.logf(LOG_ERROR, "no available nsqlookupd to query for channels to pre-create for topic %s", t.name)
	}

	// 现在已经添加了所有通道，启动该topic对应的消息泵messagePump
	t.Start()
	return t
}

// GetExistingTopic 只有在一个主题topic存在的情况下才会返回该主题topic对象
func (n *NSQD) GetExistingTopic(topicName string) (*Topic, error) {
	n.RLock()
	defer n.RUnlock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	return topic, nil
}

// DeleteExistingTopic 只在topic存在的情况下删除该topic
func (n *NSQD) DeleteExistingTopic(topicName string) error {
	n.RLock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		n.RUnlock()
		return errors.New("topic does not exist")
	}
	n.RUnlock()

	// 在关闭之前删除所有频道channel和主题topic本身。
	// (这样我们就不会留下任何信息)
	//
	// 我们在从下面的map中删除主题之前做上锁。
	// 这样，任何传入的写入都会阻塞等待锁，而不会创建一个新的主题。
	// 强制执行
	topic.Delete()

	n.Lock()
	delete(n.topicMap, topicName)
	n.Unlock()

	return nil
}

// Notify 将topic和channel对象变动情况通知到lookupLoop协程中注册或注销
func (n *NSQD) Notify(v interface{}, persist bool) {
	// 由于内存中的元数据不完整，因此在加载时不应保留元数据。nsqd将在加载后调用PersistMetadata持久元数据方法
	loading := atomic.LoadInt32(&n.isLoading) == 1
	n.waitGroup.Wrap(func() {
		// 通过在exitChan上的选择，可以保证我们不会阻止退出，见问题#123
		select {
		case <-n.exitChan: // 收到优雅结束通知,则结束此协程
		case n.notifyChan <- v: // 向通知管道发送topic或channel对象检查变动情况确定注册或者注销对应的topic或channel
			if loading || !persist {
				return
			}
			n.Lock()
			err := n.PersistMetadata()
			if err != nil {
				n.logf(LOG_ERROR, "failed to persist metadata - %s", err)
			}
			n.Unlock()
		}
	})
}

// channels 返回所有主题中的所有频道对象的列表。
func (n *NSQD) channels() []*Channel {
	var channels []*Channel
	n.RLock()
	for _, t := range n.topicMap {
		t.RLock()
		for _, c := range t.channelMap {
			channels = append(channels, c)
		}
		t.RUnlock()
	}
	n.RUnlock()
	return channels
}

// resizePool调整queueScanWorker goroutines池的大小。
//
// 1 <= pool <= min(num * 0.25, QueueScanWorkerPoolMax)
func (n *NSQD) resizePool(num int, workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	idealPoolSize := int(float64(num) * 0.25) // 队列扫描时的最大工作线程数量渠道数/4
	if idealPoolSize < 1 {
		idealPoolSize = 1
	} else if idealPoolSize > n.getOpts().QueueScanWorkerPoolMax {
		idealPoolSize = n.getOpts().QueueScanWorkerPoolMax // 控制队列扫描时的最大工作线程数量，默认4
	}
	for {
		if idealPoolSize == n.poolSize {
			break
		} else if idealPoolSize < n.poolSize {
			// contract
			closeCh <- 1
			n.poolSize--
		} else {
			// expand
			n.waitGroup.Wrap(func() {
				n.queueScanWorker(workCh, responseCh, closeCh)
			})
			n.poolSize++
		}
	}
}

// queueScanWorker 从queueScanLoop接收工作（以通道的形式），并处理延迟和飞行中的队列。
func (n *NSQD) queueScanWorker(workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	for {
		select {
		case c := <-workCh:
			now := time.Now().UnixNano()
			dirty := false
			if c.processInFlightQueue(now) {
				dirty = true
			}
			if c.processDeferredQueue(now) {
				dirty = true
			}
			responseCh <- dirty
		case <-closeCh:
			return
		}
	}
}

// queueScanLoop 在单个goroutine中运行，以处理消费中和延迟优先级队列。
// 它管理一个队列ScanWorker池（可配置的最大值为
// QueueScanWorkerPoolMax（默认值：4））同时处理通道。
//
// 它复制Redis的概率过期算法：它每隔QueueScanInterval（默认值：100ms）就随机选择
// QueueScanSelectionCount（默认值：20）个本地缓存列表中的频道加入到处理频道channel方法中检查处理
//
// 每隔QueueScanRefreshInterval（默认值：5s）将更新一次频道channel列表。
//
// 如果任何一个队列都有工作要做，则通道被认为是“脏的”。
//
// 如果所选频道的QueueScanDirtyPercent（默认值：25%）为脏通道，则循环随机处理选中的频道列表，直到小于QueueScanDirtyPercent值。
func (n *NSQD) queueScanLoop() {
	workCh := make(chan *Channel, n.getOpts().QueueScanSelectionCount) // 根据queue-scan-selection-count生成指定大小需要检查处理频道channel的通道
	responseCh := make(chan bool, n.getOpts().QueueScanSelectionCount) // 根据queue-scan-selection-count生成指定大小需要接受处理频道channel结果的通道
	closeCh := make(chan int)                                          // 生成关闭通道，根据此通道可以通知到所有处理频道的方法中

	workTicker := time.NewTicker(n.getOpts().QueueScanInterval)           // 生成轮询检查定时器（固定100毫秒）
	refreshTicker := time.NewTicker(n.getOpts().QueueScanRefreshInterval) // 生成轮询刷新定时器（固定5秒）

	channels := n.channels()                                 // 获取nsqd对象下的所有频道channel对象
	n.resizePool(len(channels), workCh, responseCh, closeCh) // 开启指定大小（1 <= pool <= min(len(channels) * 0.25, QueueScanWorkerPoolMax)）的处理频道channel方法（协程方式启动）

	for {
		select {
		case <-workTicker.C: // 到达轮询检查时间片，检查是否处理的频道channel数是否为0，若为0跳过后续处理逻辑等待下一个触发通道通知
			if len(channels) == 0 {
				continue
			}
		case <-refreshTicker.C: // 到达刷新时间片，重新获取所有频道channel对象并按新频道数开放需要的处理频道channel方法（协程方式启动）
			channels = n.channels()
			n.resizePool(len(channels), workCh, responseCh, closeCh)
			continue
		case <-n.exitChan: // 收到退出信号，走退出流程
			goto exit
		}

		// 根据queue-scan-selection-count和频道channel的长度生成最小num（用于随机频道加入到处理频道channel方法中检查处理）
		num := n.getOpts().QueueScanSelectionCount
		if num > len(channels) {
			num = len(channels)
		}

	loop:
		for _, i := range util.UniqRands(num, len(channels)) { // 生成num个随机channels的下标数组
			workCh <- channels[i] // 通过通道放到处理频道channel方法中检查处理
		}

		numDirty := 0 // 用于统计此次处理中频道channel列表中需要处理的个数
		for i := 0; i < num; i++ {
			if <-responseCh {
				numDirty++
			}
		}

		if float64(numDirty)/float64(num) > n.getOpts().QueueScanDirtyPercent { // 当处理命中率达到QueueScanDirtyPercent（默认0.25）时继续轮询处理
			goto loop
		}
	}

exit:
	n.logf(LOG_INFO, "QUEUESCAN: closing")
	close(closeCh)
	workTicker.Stop()
	refreshTicker.Stop()
}

// buildTLSConfig 构建tls.Config对象
func buildTLSConfig(opts *Options) (*tls.Config, error) {
	var tlsConfig *tls.Config

	if opts.TLSCert == "" && opts.TLSKey == "" { // 没有证书信息时,跳过构建操作
		return nil, nil
	}

	tlsClientAuthPolicy := tls.VerifyClientCertIfGiven // VerifyClientCertIfGiven 表示在握手过程中应该请求客户证书，但不要求客户发送证书。如果客户端确实发送了证书，则要求它是有效的。

	cert, err := tls.LoadX509KeyPair(opts.TLSCert, opts.TLSKey) // 生成证书对象
	if err != nil {
		return nil, err
	}
	switch opts.TLSClientAuthPolicy {
	case "require": // RequireAnyClientCert 表示在握手过程中应该请求客户证书，并且要求客户至少发送一份证书，但不要求该证书必须有效。
		tlsClientAuthPolicy = tls.RequireAnyClientCert
	case "require-verify": // RequireAndVerifyClientCert 表示在握手过程中应该请求客户证书，并且要求客户至少发送一份有效的证书。
		tlsClientAuthPolicy = tls.RequireAndVerifyClientCert
	default: // NoClientCert 表示在握手过程中不应该请求客户证书，如果有任何证书被发送，它们将不会被验证。
		tlsClientAuthPolicy = tls.NoClientCert
	}

	tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tlsClientAuthPolicy,
		MinVersion:   opts.TLSMinVersion,
	}

	if opts.TLSRootCAFile != "" {
		tlsCertPool := x509.NewCertPool()
		caCertFile, err := os.ReadFile(opts.TLSRootCAFile)
		if err != nil {
			return nil, err
		}
		if !tlsCertPool.AppendCertsFromPEM(caCertFile) {
			return nil, errors.New("failed to append certificate to pool")
		}
		tlsConfig.ClientCAs = tlsCertPool
	}

	return tlsConfig, nil
}

// IsAuthEnabled 是否需要认证
func (n *NSQD) IsAuthEnabled() bool {
	return len(n.getOpts().AuthHTTPAddresses) != 0
}

// Context返回一个当nsqd启动停止时可以被取消的上下文。
func (n *NSQD) Context() context.Context {
	return n.ctx
}
