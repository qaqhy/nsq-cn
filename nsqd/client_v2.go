package nsqd

import (
	"bufio"
	"compress/flate"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	"github.com/nsqio/nsq/internal/auth"
)

const defaultBufferSize = 16 * 1024

const (
	stateInit = iota
	stateDisconnected
	stateConnected
	stateSubscribed
	stateClosing
)

type identifyDataV2 struct {
	ClientID            string `json:"client_id"`
	Hostname            string `json:"hostname"`
	HeartbeatInterval   int    `json:"heartbeat_interval"`
	OutputBufferSize    int    `json:"output_buffer_size"`
	OutputBufferTimeout int    `json:"output_buffer_timeout"`
	FeatureNegotiation  bool   `json:"feature_negotiation"`
	TLSv1               bool   `json:"tls_v1"`
	Deflate             bool   `json:"deflate"`
	DeflateLevel        int    `json:"deflate_level"`
	Snappy              bool   `json:"snappy"`
	SampleRate          int32  `json:"sample_rate"`
	UserAgent           string `json:"user_agent"`
	MsgTimeout          int    `json:"msg_timeout"`
}

type identifyEvent struct {
	OutputBufferTimeout time.Duration
	HeartbeatInterval   time.Duration
	SampleRate          int32
	MsgTimeout          time.Duration
}

type PubCount struct {
	Topic string `json:"topic"`
	Count uint64 `json:"count"`
}

type ClientV2Stats struct {
	ClientID        string `json:"client_id"`
	Hostname        string `json:"hostname"`
	Version         string `json:"version"`
	RemoteAddress   string `json:"remote_address"`
	State           int32  `json:"state"`
	ReadyCount      int64  `json:"ready_count"`
	InFlightCount   int64  `json:"in_flight_count"`
	MessageCount    uint64 `json:"message_count"`
	FinishCount     uint64 `json:"finish_count"`
	RequeueCount    uint64 `json:"requeue_count"`
	ConnectTime     int64  `json:"connect_ts"`
	SampleRate      int32  `json:"sample_rate"`
	Deflate         bool   `json:"deflate"`
	Snappy          bool   `json:"snappy"`
	UserAgent       string `json:"user_agent"`
	Authed          bool   `json:"authed,omitempty"`
	AuthIdentity    string `json:"auth_identity,omitempty"`
	AuthIdentityURL string `json:"auth_identity_url,omitempty"`

	PubCounts []PubCount `json:"pub_counts,omitempty"`

	TLS                           bool   `json:"tls"`
	CipherSuite                   string `json:"tls_cipher_suite"`
	TLSVersion                    string `json:"tls_version"`
	TLSNegotiatedProtocol         string `json:"tls_negotiated_protocol"`
	TLSNegotiatedProtocolIsMutual bool   `json:"tls_negotiated_protocol_is_mutual"`
}

func (s ClientV2Stats) String() string {
	connectTime := time.Unix(s.ConnectTime, 0)
	duration := time.Since(connectTime).Truncate(time.Second)

	_, port, _ := net.SplitHostPort(s.RemoteAddress)
	id := fmt.Sprintf("%s:%s %s", s.Hostname, port, s.UserAgent)

	// producer
	if len(s.PubCounts) > 0 {
		var total uint64
		var topicOut []string
		for _, v := range s.PubCounts {
			total += v.Count
			topicOut = append(topicOut, fmt.Sprintf("%s=%d", v.Topic, v.Count))
		}
		return fmt.Sprintf("[%s %-21s] msgs: %-8d topics: %s connected: %s",
			s.Version,
			id,
			total,
			strings.Join(topicOut, ","),
			duration,
		)
	}

	// consumer
	return fmt.Sprintf("[%s %-21s] state: %d inflt: %-4d rdy: %-4d fin: %-8d re-q: %-8d msgs: %-8d connected: %s",
		s.Version,
		id,
		s.State,
		s.InFlightCount,
		s.ReadyCount,
		s.FinishCount,
		s.RequeueCount,
		s.MessageCount,
		duration,
	)
}

type clientV2 struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	ReadyCount    int64 // 客户端设置的读取数据量大小
	InFlightCount int64 // 客户端消费中队列的实际大小
	MessageCount  uint64
	FinishCount   uint64
	RequeueCount  uint64

	pubCounts map[string]uint64

	writeLock sync.RWMutex
	metaLock  sync.RWMutex

	ID        int64
	nsqd      *NSQD
	UserAgent string

	// original connection
	net.Conn

	// connections based on negotiated features
	tlsConn     *tls.Conn
	flateWriter *flate.Writer

	// reading/writing interfaces
	Reader *bufio.Reader
	Writer *bufio.Writer

	OutputBufferSize    int
	OutputBufferTimeout time.Duration

	HeartbeatInterval time.Duration

	MsgTimeout time.Duration

	State          int32
	ConnectTime    time.Time
	Channel        *Channel
	ReadyStateChan chan int
	ExitChan       chan int

	ClientID string
	Hostname string

	SampleRate int32

	IdentifyEventChan chan identifyEvent
	SubEventChan      chan *Channel

	TLS     int32
	Snappy  int32
	Deflate int32

	// re-usable buffer for reading the 4-byte lengths off the wire
	lenBuf   [4]byte
	lenSlice []byte

	AuthSecret string
	AuthState  *auth.State
}

func newClientV2(id int64, conn net.Conn, nsqd *NSQD) *clientV2 {
	var identifier string
	if conn != nil {
		identifier, _, _ = net.SplitHostPort(conn.RemoteAddr().String())
	}

	c := &clientV2{
		ID:   id,
		nsqd: nsqd,

		Conn: conn,

		Reader: bufio.NewReaderSize(conn, defaultBufferSize),
		Writer: bufio.NewWriterSize(conn, defaultBufferSize),

		OutputBufferSize:    defaultBufferSize,
		OutputBufferTimeout: nsqd.getOpts().OutputBufferTimeout,

		MsgTimeout: nsqd.getOpts().MsgTimeout,

		// ReadyStateChan has a buffer of 1 to guarantee that in the event
		// there is a race the state update is not lost
		ReadyStateChan: make(chan int, 1),
		ExitChan:       make(chan int),
		ConnectTime:    time.Now(),
		State:          stateInit,

		ClientID: identifier,
		Hostname: identifier,

		SubEventChan:      make(chan *Channel, 1),
		IdentifyEventChan: make(chan identifyEvent, 1),

		// heartbeats are client configurable but default to 30s
		HeartbeatInterval: nsqd.getOpts().ClientTimeout / 2,

		pubCounts: make(map[string]uint64),
	}
	c.lenSlice = c.lenBuf[:]
	return c
}

func (c *clientV2) String() string {
	return c.RemoteAddr().String()
}

func (c *clientV2) Type() int {
	c.metaLock.RLock()
	hasPublished := len(c.pubCounts) > 0
	c.metaLock.RUnlock()
	if hasPublished {
		return typeProducer
	}
	return typeConsumer
}

// Identify 设置客户端鉴权数据
func (c *clientV2) Identify(data identifyDataV2) error {
	c.nsqd.logf(LOG_INFO, "[%s] IDENTIFY: %+v", c, data)

	c.metaLock.Lock()
	c.ClientID = data.ClientID
	c.Hostname = data.Hostname
	c.UserAgent = data.UserAgent
	c.metaLock.Unlock()

	// 更新设置合法的心跳间隔时间
	err := c.SetHeartbeatInterval(data.HeartbeatInterval)
	if err != nil {
		return err
	}

	// 更新设置合法的输出流缓冲区大小和输出流间隔时间
	err = c.SetOutputBuffer(data.OutputBufferSize, data.OutputBufferTimeout)
	if err != nil {
		return err
	}

	// 更新设置采样率,范围[0,99]
	err = c.SetSampleRate(data.SampleRate)
	if err != nil {
		return err
	}

	// 更新设置消息消费的最长时间
	err = c.SetMsgTimeout(data.MsgTimeout)
	if err != nil {
		return err
	}

	// 根据上面传入的鉴权信息生成鉴权数据对象
	ie := identifyEvent{
		OutputBufferTimeout: c.OutputBufferTimeout,
		HeartbeatInterval:   c.HeartbeatInterval,
		SampleRate:          c.SampleRate,
		MsgTimeout:          c.MsgTimeout,
	}

	// 更新客户端连接对象的信息泵,如果已更新过将不再更新,需等待此客户端连接重新建立才能刷新
	select {
	case c.IdentifyEventChan <- ie:
	default:
	}

	return nil
}

func (c *clientV2) Stats(topicName string) ClientStats {
	c.metaLock.RLock()
	clientID := c.ClientID
	hostname := c.Hostname
	userAgent := c.UserAgent
	var identity string
	var identityURL string
	if c.AuthState != nil {
		identity = c.AuthState.Identity
		identityURL = c.AuthState.IdentityURL
	}
	pubCounts := make([]PubCount, 0, len(c.pubCounts))
	for topic, count := range c.pubCounts {
		if len(topicName) > 0 && topic != topicName {
			continue
		}
		pubCounts = append(pubCounts, PubCount{
			Topic: topic,
			Count: count,
		})
		break
	}
	c.metaLock.RUnlock()
	stats := ClientV2Stats{
		Version:         "V2",
		RemoteAddress:   c.RemoteAddr().String(),
		ClientID:        clientID,
		Hostname:        hostname,
		UserAgent:       userAgent,
		State:           atomic.LoadInt32(&c.State),
		ReadyCount:      atomic.LoadInt64(&c.ReadyCount),
		InFlightCount:   atomic.LoadInt64(&c.InFlightCount),
		MessageCount:    atomic.LoadUint64(&c.MessageCount),
		FinishCount:     atomic.LoadUint64(&c.FinishCount),
		RequeueCount:    atomic.LoadUint64(&c.RequeueCount),
		ConnectTime:     c.ConnectTime.Unix(),
		SampleRate:      atomic.LoadInt32(&c.SampleRate),
		TLS:             atomic.LoadInt32(&c.TLS) == 1,
		Deflate:         atomic.LoadInt32(&c.Deflate) == 1,
		Snappy:          atomic.LoadInt32(&c.Snappy) == 1,
		Authed:          c.HasAuthorizations(),
		AuthIdentity:    identity,
		AuthIdentityURL: identityURL,
		PubCounts:       pubCounts,
	}
	if stats.TLS {
		p := prettyConnectionState{c.tlsConn.ConnectionState()}
		stats.CipherSuite = p.GetCipherSuite()
		stats.TLSVersion = p.GetVersion()
		stats.TLSNegotiatedProtocol = p.NegotiatedProtocol
		stats.TLSNegotiatedProtocolIsMutual = p.NegotiatedProtocolIsMutual
	}
	return stats
}

// struct to convert from integers to the human readable strings
type prettyConnectionState struct {
	tls.ConnectionState
}

func (p *prettyConnectionState) GetCipherSuite() string {
	switch p.CipherSuite {
	case tls.TLS_RSA_WITH_RC4_128_SHA:
		return "TLS_RSA_WITH_RC4_128_SHA"
	case tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA:
		return "TLS_RSA_WITH_3DES_EDE_CBC_SHA"
	case tls.TLS_RSA_WITH_AES_128_CBC_SHA:
		return "TLS_RSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_RSA_WITH_AES_256_CBC_SHA:
		return "TLS_RSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA:
		return "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA:
		return "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA:
		return "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA:
		return "TLS_ECDHE_RSA_WITH_RC4_128_SHA"
	case tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256:
		return "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256:
		return "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"
	}
	return fmt.Sprintf("Unknown %d", p.CipherSuite)
}

func (p *prettyConnectionState) GetVersion() string {
	switch p.Version {
	case tls.VersionTLS10:
		return "TLS1.0"
	case tls.VersionTLS11:
		return "TLS1.1"
	case tls.VersionTLS12:
		return "TLS1.2"
	case tls.VersionTLS13:
		return "TLS1.3"
	default:
		return fmt.Sprintf("Unknown %d", p.Version)
	}
}

// IsReadyForMessages 判断此客户端对应的频道是否可以继续消费任务
func (c *clientV2) IsReadyForMessages() bool {
	if c.Channel.IsPaused() { // 此频道已停止消费任务
		return false
	}

	readyCount := atomic.LoadInt64(&c.ReadyCount)       // 读取客户端设置的读取数据量大小
	inFlightCount := atomic.LoadInt64(&c.InFlightCount) // 读取客户端消费中队列的实际大小

	c.nsqd.logf(LOG_DEBUG, "[%s] state rdy: %4d inflt: %4d", c, readyCount, inFlightCount)

	if inFlightCount >= readyCount || readyCount <= 0 { // 如果消费中队列
		return false
	}

	return true
}

// SetReadyCount 设置客户端的读取数据量大小,若设置值与旧值不通则发送读取状态更新的通知
func (c *clientV2) SetReadyCount(count int64) {
	oldCount := atomic.SwapInt64(&c.ReadyCount, count) // 设置客户端的读取数据量大小

	if oldCount != count {
		c.tryUpdateReadyState()
	}
}

// tryUpdateReadyState 发送读取状态更新的通知
func (c *clientV2) tryUpdateReadyState() {
	// you can always *try* to write to ReadyStateChan because in the cases
	// where you cannot the message pump loop would have iterated anyway.
	// the atomic integer operations guarantee correctness of the value.
	select { // 通道有监听下才发送,没有则忽略
	case c.ReadyStateChan <- 1:
	default:
	}
}

// 更新
func (c *clientV2) FinishedMessage() {
	atomic.AddUint64(&c.FinishCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

func (c *clientV2) Empty() {
	atomic.StoreInt64(&c.InFlightCount, 0)
	c.tryUpdateReadyState()
}

func (c *clientV2) SendingMessage() {
	atomic.AddInt64(&c.InFlightCount, 1)
	atomic.AddUint64(&c.MessageCount, 1)
}

func (c *clientV2) PublishedMessage(topic string, count uint64) {
	c.metaLock.Lock()
	c.pubCounts[topic] += count
	c.metaLock.Unlock()
}

func (c *clientV2) TimedOutMessage() {
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

func (c *clientV2) RequeuedMessage() {
	atomic.AddUint64(&c.RequeueCount, 1)  // 重新入队数减一
	atomic.AddInt64(&c.InFlightCount, -1) // 消费队列数减一
	c.tryUpdateReadyState()
}

func (c *clientV2) StartClose() {
	// Force the client into ready 0
	c.SetReadyCount(0)
	// mark this client as closing
	atomic.StoreInt32(&c.State, stateClosing)
}

func (c *clientV2) Pause() {
	c.tryUpdateReadyState()
}

func (c *clientV2) UnPause() {
	c.tryUpdateReadyState()
}

// SetHeartbeatInterval 更新设置合法的心跳间隔时间
func (c *clientV2) SetHeartbeatInterval(desiredInterval int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	switch {
	case desiredInterval == -1:
		c.HeartbeatInterval = 0
	case desiredInterval == 0:
		// do nothing (use default)
	case desiredInterval >= 1000 &&
		desiredInterval <= int(c.nsqd.getOpts().MaxHeartbeatInterval/time.Millisecond):
		c.HeartbeatInterval = time.Duration(desiredInterval) * time.Millisecond
	default:
		return fmt.Errorf("heartbeat interval (%d) is invalid", desiredInterval)
	}

	return nil
}

// SetOutputBuffer 更新设置合法的输出流缓冲区大小和输出流间隔时间
func (c *clientV2) SetOutputBuffer(desiredSize int, desiredTimeout int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	switch {
	case desiredTimeout == -1:
		c.OutputBufferTimeout = 0
	case desiredTimeout == 0:
		// 不执行任何操作（使用默认值）
	case desiredTimeout >= int(c.nsqd.getOpts().MinOutputBufferTimeout/time.Millisecond) &&
		desiredTimeout <= int(c.nsqd.getOpts().MaxOutputBufferTimeout/time.Millisecond):

		c.OutputBufferTimeout = time.Duration(desiredTimeout) * time.Millisecond
	default:
		return fmt.Errorf("output buffer timeout (%d) is invalid", desiredTimeout)
	}

	switch {
	case desiredSize == -1:
		// 实际上没有缓冲区（每次写入都将直接进入封装的net.Conn）
		c.OutputBufferSize = 1
		c.OutputBufferTimeout = 0
	case desiredSize == 0:
		// 不执行任何操作（使用默认值）
	case desiredSize >= 64 && desiredSize <= int(c.nsqd.getOpts().MaxOutputBufferSize):
		c.OutputBufferSize = desiredSize
	default:
		return fmt.Errorf("output buffer size (%d) is invalid", desiredSize)
	}

	if desiredSize != 0 { // 确认修改了缓冲区大小后需要刷新一下写入流对象并生成一个新的写入流对象
		err := c.Writer.Flush()
		if err != nil {
			return err
		}
		c.Writer = bufio.NewWriterSize(c.Conn, c.OutputBufferSize)
	}

	return nil
}

// SetSampleRate 更新设置采样率,范围[0,99]
func (c *clientV2) SetSampleRate(sampleRate int32) error {
	if sampleRate < 0 || sampleRate > 99 {
		return fmt.Errorf("sample rate (%d) is invalid", sampleRate)
	}
	atomic.StoreInt32(&c.SampleRate, sampleRate)
	return nil
}

// SetMsgTimeout 更新设置消息消费的最长时间
func (c *clientV2) SetMsgTimeout(msgTimeout int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	switch {
	case msgTimeout == 0:
		// 不执行任何操作（使用默认值）
	case msgTimeout >= 1000 &&
		msgTimeout <= int(c.nsqd.getOpts().MaxMsgTimeout/time.Millisecond):
		c.MsgTimeout = time.Duration(msgTimeout) * time.Millisecond
	default:
		return fmt.Errorf("msg timeout (%d) is invalid", msgTimeout)
	}

	return nil
}

// UpgradeTLS 升级TLS协议并更新客户端连接对象的读取流和写入流对象
func (c *clientV2) UpgradeTLS() error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	tlsConn := tls.Server(c.Conn, c.nsqd.tlsConfig)
	tlsConn.SetDeadline(time.Now().Add(5 * time.Second))
	err := tlsConn.Handshake()
	if err != nil {
		return err
	}
	c.tlsConn = tlsConn

	c.Reader = bufio.NewReaderSize(c.tlsConn, defaultBufferSize)  // 增加TLS协议生成读取流对象,最大大小限制16KB
	c.Writer = bufio.NewWriterSize(c.tlsConn, c.OutputBufferSize) // 增加TLS协议生成写入流对象,最大大小限制设置值(客户端设置,不能超过nsqd服务端的限制范围[64B,64KB])

	atomic.StoreInt32(&c.TLS, 1)

	return nil
}

// UpgradeDeflate 更新设置Deflate压缩算法的压缩等级,同时更新读取流和写入流对象
func (c *clientV2) UpgradeDeflate(level int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	conn := c.Conn
	if c.tlsConn != nil {
		conn = c.tlsConn
	}

	c.Reader = bufio.NewReaderSize(flate.NewReader(conn), defaultBufferSize)

	fw, _ := flate.NewWriter(conn, level)
	c.flateWriter = fw
	c.Writer = bufio.NewWriterSize(fw, c.OutputBufferSize)

	atomic.StoreInt32(&c.Deflate, 1)

	return nil
}

// UpgradeSnappy 更新设置为Snappy压缩算法,同时更新读取流和写入流对象
func (c *clientV2) UpgradeSnappy() error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	conn := c.Conn
	if c.tlsConn != nil {
		conn = c.tlsConn
	}

	c.Reader = bufio.NewReaderSize(snappy.NewReader(conn), defaultBufferSize)
	//lint:ignore SA1019 NewWriter is deprecated by NewBufferedWriter, but we're doing our own buffering
	c.Writer = bufio.NewWriterSize(snappy.NewWriter(conn), c.OutputBufferSize)

	atomic.StoreInt32(&c.Snappy, 1)

	return nil
}

// Flush 刷新写入流数据
func (c *clientV2) Flush() error {
	var zeroTime time.Time
	if c.HeartbeatInterval > 0 {
		c.SetWriteDeadline(time.Now().Add(c.HeartbeatInterval)) // 设置写入数据的截至时间
	} else {
		c.SetWriteDeadline(zeroTime) // 设置写入数据无截至时间
	}

	err := c.Writer.Flush() // Flush将任何缓冲的数据写入底层的io.Writer中。
	if err != nil {
		return err
	}

	if c.flateWriter != nil {
		return c.flateWriter.Flush()
	}

	return nil
}

// QueryAuthd 查询并获取鉴权
func (c *clientV2) QueryAuthd() error {
	remoteIP := ""
	if c.RemoteAddr().Network() == "tcp" {
		ip, _, err := net.SplitHostPort(c.String())
		if err != nil {
			return err
		}
		remoteIP = ip
	}

	tlsEnabled := atomic.LoadInt32(&c.TLS) == 1
	commonName := ""
	if tlsEnabled {
		tlsConnState := c.tlsConn.ConnectionState()
		if len(tlsConnState.PeerCertificates) > 0 {
			commonName = tlsConnState.PeerCertificates[0].Subject.CommonName
		}
	}

	authState, err := auth.QueryAnyAuthd(c.nsqd.getOpts().AuthHTTPAddresses,
		remoteIP, tlsEnabled, commonName, c.AuthSecret,
		c.nsqd.getOpts().HTTPClientConnectTimeout,
		c.nsqd.getOpts().HTTPClientRequestTimeout)
	if err != nil {
		return err
	}
	c.AuthState = authState
	return nil
}

func (c *clientV2) Auth(secret string) error {
	c.AuthSecret = secret
	return c.QueryAuthd()
}

func (c *clientV2) IsAuthorized(topic, channel string) (bool, error) {
	if c.AuthState == nil {
		return false, nil
	}
	if c.AuthState.IsExpired() {
		err := c.QueryAuthd()
		if err != nil {
			return false, err
		}
	}
	if c.AuthState.IsAllowed(topic, channel) {
		return true, nil
	}
	return false, nil
}

func (c *clientV2) HasAuthorizations() bool {
	if c.AuthState != nil {
		return len(c.AuthState.Authorizations) != 0
	}
	return false
}
