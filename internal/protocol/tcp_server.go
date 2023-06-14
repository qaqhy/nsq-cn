package protocol

import (
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"

	"github.com/nsqio/nsq/internal/lg"
)

type TCPHandler interface {
	Handle(net.Conn)
}

func TCPServer(listener net.Listener, handler TCPHandler, logf lg.AppLogFunc) error {
	logf(lg.INFO, "TCP: listening on %s", listener.Addr())

	var wg sync.WaitGroup

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			// net.Error.Temporary()已弃用，但对accept有效。这是一种避免静态检查错误的破解方法
			if te, ok := err.(interface{ Temporary() bool }); ok && te.Temporary() { // 触发此错误,重新监听后面的客户端连接
				logf(lg.WARN, "temporary Accept() failure - %s", err)
				runtime.Gosched() // 让出CPU让其他协程先跑
				continue
			}
			// 没有直接的方法来检测这个错误，因为它没有被暴露,这里使用字符串包含方式判断错误内容
			if !strings.Contains(err.Error(), "use of closed network connection") {
				return fmt.Errorf("listener.Accept() error - %s", err)
			}
			break // 其他错误将停止监听TCP客户端的请求
		}

		wg.Add(1)
		go func() { // 协程方式开启此连接的交互处理
			handler.Handle(clientConn)
			wg.Done()
		}()
	}

	// 等待返回，直到所有处理程序的协程goroutine完成。
	wg.Wait()

	logf(lg.INFO, "TCP: closing %s", listener.Addr())

	return nil
}
