package nsqd

import (
	"bytes"
	"sync"
)

// 重用字节缓冲区,避免频繁创建和销毁资源导致GC缓慢进而引起整体性能的下降
var bp sync.Pool

func init() {
	bp.New = func() interface{} {
		return &bytes.Buffer{}
	}
}

// bufferPoolGet 获取池中已有的缓冲区对象,若无缓冲区对象则调用New方法开辟一个新的缓冲区对象
func bufferPoolGet() *bytes.Buffer {
	return bp.Get().(*bytes.Buffer)
}

// bufferPoolPut 将缓冲对象保存暂存到池中
func bufferPoolPut(b *bytes.Buffer) {
	b.Reset()
	bp.Put(b)
}
