package nsqd

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/lg"
)

// lookupPeer is a low-level type for connecting/reading/writing to nsqlookupd
//
// A lookupPeer instance is designed to connect lazily to nsqlookupd and reconnect
// gracefully (i.e. it is all handled by the library).  Clients can simply use the
// Command interface to perform a round-trip.
type lookupPeer struct {
	logf            lg.AppLogFunc
	addr            string
	conn            net.Conn
	state           int32
	connectCallback func(*lookupPeer)
	maxBodySize     int64
	Info            peerInfo
}

// peerInfo contains metadata for a lookupPeer instance (and is JSON marshalable)
type peerInfo struct {
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
	BroadcastAddress string `json:"broadcast_address"`
}

// newLookupPeer 创建一个新的lookupPeer实例，连接到提供的地址。
//
// 提供的connectCallback方法将在实例每次连接时被调用。
func newLookupPeer(addr string, maxBodySize int64, l lg.AppLogFunc, connectCallback func(*lookupPeer)) *lookupPeer {
	return &lookupPeer{
		logf:            l,
		addr:            addr,
		state:           stateDisconnected, // 初始化未断开连接状态
		maxBodySize:     maxBodySize,
		connectCallback: connectCallback,
	}
}

// 连接将拨打指定的地址并有设置最长超时时间为1秒。
func (lp *lookupPeer) Connect() error {
	lp.logf(lg.INFO, "LOOKUP connecting to %s", lp.addr)
	conn, err := net.DialTimeout("tcp", lp.addr, time.Second)
	if err != nil {
		return err
	}
	lp.conn = conn
	return nil
}

// String returns the specified address
func (lp *lookupPeer) String() string {
	return lp.addr
}

// Read implements the io.Reader interface, adding deadlines
func (lp *lookupPeer) Read(data []byte) (int, error) {
	lp.conn.SetReadDeadline(time.Now().Add(time.Second))
	return lp.conn.Read(data)
}

// Write implements the io.Writer interface, adding deadlines
func (lp *lookupPeer) Write(data []byte) (int, error) {
	lp.conn.SetWriteDeadline(time.Now().Add(time.Second))
	return lp.conn.Write(data)
}

// Close 关闭连接对象并设置为断开连接状态
func (lp *lookupPeer) Close() error {
	lp.state = stateDisconnected
	if lp.conn != nil {
		return lp.conn.Close()
	}
	return nil
}

// Command performs a round-trip for the specified Command.
//
// It will lazily connect to nsqlookupd and gracefully handle
// reconnecting in the event of a failure.
//
// It returns the response from nsqlookupd as []byte
func (lp *lookupPeer) Command(cmd *nsq.Command) ([]byte, error) {
	initialState := lp.state
	if lp.state != stateConnected { // 非连接状态则执行以下流程
		err := lp.Connect() // 初始化连接对象
		if err != nil {
			return nil, err
		}
		lp.state = stateConnected // 更新状态为连接状态
		_, err = lp.Write(nsq.MagicV1)
		if err != nil {
			lp.Close()
			return nil, err
		}
		if initialState == stateDisconnected { // 如果是断开连接状态则同步此nsqd服务上所有的topic和channel信息到此服务发现上
			lp.connectCallback(lp)
		}
		if lp.state != stateConnected { // 如果连接状态非连接状态则返回错误信息
			return nil, fmt.Errorf("lookupPeer connectCallback() failed")
		}
	}
	if cmd == nil { // 空命令下则直接返回
		return nil, nil
	}
	_, err := cmd.WriteTo(lp) // 将命令内容写到lp流中
	if err != nil {
		lp.Close()
		return nil, err
	}
	resp, err := readResponseBounded(lp, lp.maxBodySize) // 读取流返回数据
	if err != nil {
		lp.Close()
		return nil, err
	}
	return resp, nil
}

func readResponseBounded(r io.Reader, limit int64) ([]byte, error) {
	var msgSize int32

	// 大端方式读取消息的长度
	err := binary.Read(r, binary.BigEndian, &msgSize)
	if err != nil {
		return nil, err
	}

	if int64(msgSize) > limit { // 内容大小超过限制则返回错误信息
		return nil, fmt.Errorf("response body size (%d) is greater than limit (%d)",
			msgSize, limit)
	}

	// 二进制消息数据
	buf := make([]byte, msgSize)
	_, err = io.ReadFull(r, buf) // 读取所有的数据到buf对象中
	if err != nil {
		return nil, err
	}

	return buf, nil
}
