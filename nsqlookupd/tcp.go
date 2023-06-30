package nsqlookupd

import (
	"io"
	"net"
	"sync"

	"github.com/nsqio/nsq/internal/protocol"
)

type tcpServer struct {
	nsqlookupd *NSQLookupd
	conns      sync.Map
}

func (p *tcpServer) Handle(conn net.Conn) {
	p.nsqlookupd.logf(LOG_INFO, "TCP: new client(%s)", conn.RemoteAddr())

	// 客户端通过发送一个4字节的序列来初始化自己，表明它通信使用的协议版本
	// 这将使我们能够优雅地升级协议，从面向text/line的协议升级到任何协议。
	buf := make([]byte, 4)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		p.nsqlookupd.logf(LOG_ERROR, "failed to read protocol version - %s", err)
		conn.Close()
		return
	}
	protocolMagic := string(buf)

	p.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): desired protocol magic '%s'",
		conn.RemoteAddr(), protocolMagic) // 打印连接的客户端IP及端口信息和所请求使用的协议版本

	var prot protocol.Protocol
	switch protocolMagic {
	case "  V1": // V2协议将初始化LookupProtocolV1对象
		prot = &LookupProtocolV1{nsqlookupd: p.nsqlookupd}
	default: // 其他协议未支持,所以返回E_BAD_PROTOCOL信息并主动关闭客户端连接对象
		protocol.SendResponse(conn, []byte("E_BAD_PROTOCOL"))
		conn.Close()
		p.nsqlookupd.logf(LOG_ERROR, "client(%s) bad protocol magic '%s'",
			conn.RemoteAddr(), protocolMagic)
		return
	}

	client := prot.NewClient(conn)           // 根据连接对象初始化TCP协议下的处理客户端对象
	p.conns.Store(conn.RemoteAddr(), client) // 存储更新到连接对象Map中

	err = prot.IOLoop(client) // 调用此客户端连接处理的核心逻辑
	if err != nil {
		p.nsqlookupd.logf(LOG_ERROR, "client(%s) - %s", conn.RemoteAddr(), err)
	}

	p.conns.Delete(conn.RemoteAddr()) // 客户端连接对象退出则从连接对象Map中清理掉
	client.Close()                    // 关闭客户端连接对象
}

func (p *tcpServer) Close() {
	p.conns.Range(func(k, v interface{}) bool {
		v.(protocol.Client).Close()
		return true
	})
}
