package nsqd

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/version"
)

const (
	frameTypeResponse int32 = 0
	frameTypeError    int32 = 1
	frameTypeMessage  int32 = 2
)

var separatorBytes = []byte(" ")
var heartbeatBytes = []byte("_heartbeat_")
var okBytes = []byte("OK")

type protocolV2 struct {
	nsqd *NSQD
}

// NewClient 生成V2协议处理客户端
func (p *protocolV2) NewClient(conn net.Conn) protocol.Client {
	clientID := atomic.AddInt64(&p.nsqd.clientIDSequence, 1)
	return newClientV2(clientID, conn, p.nsqd)
}

// IOLoop nsqd TCP处理的核心逻辑
// 需要注意的是每个客户端连接都是单线程调用不能并发调用(客户端请求后必须等到服务端响应后才能做后续请求)
func (p *protocolV2) IOLoop(c protocol.Client) error {
	var err error
	var line []byte
	var zeroTime time.Time

	client := c.(*clientV2)

	// 同步启动messagePump，以保证它有机会初始化源自客户端属性的goroutine本地状态，
	// 并避免与IDENTIFY的潜在竞赛（客户端可能已经改变或禁用上述属性）。
	messagePumpStartedChan := make(chan bool)
	go p.messagePump(client, messagePumpStartedChan) // 协程方式启动tcp消息泵方法
	<-messagePumpStartedChan                         // 等待消息泵的启动完成通知

	for {
		if client.HeartbeatInterval > 0 { // 心跳是客户端可配置的，但默认为30秒
			client.SetReadDeadline(time.Now().Add(client.HeartbeatInterval * 2)) // 设置客户端连接读取的超时时间为60秒
		} else {
			client.SetReadDeadline(zeroTime) // 设置客户端连接读取无超时限制
		}

		// 每次请求时，仅读取到\n截至，ReadSlice都不会为数据分配新的空间，也就是说，返回的切片line只在下次调用之前有效。
		line, err = client.Reader.ReadSlice('\n')
		if err != nil {
			if err == io.EOF { // 客户端连接优雅结束
				err = nil
			} else {
				err = fmt.Errorf("failed to read command - %s", err)
			}
			break
		}

		// trim the '\n' | 删除末尾的\n
		line = line[:len(line)-1]
		// optionally trim the '\r' | 删除末尾的\r
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		params := bytes.Split(line, separatorBytes) // 按separatorBytes切割出每个参数

		p.nsqd.logf(LOG_DEBUG, "PROTOCOL(V2): [%s] %s", client, params) // 输出的此客户端连接交互提交的参数内容

		var response []byte
		response, err = p.Exec(client, params) // 分发到路由上处理客户端请求内容
		if err != nil {                        // 路由中执行出现异常
			ctx := ""
			// 首先将err强制类型转换为protocol.ChildErr类型。
			// 然后，从这个child error中获取到它的parentErr父错误，判断是否存在。
			// 如果存在父错误，则在错误信息前面添加一个"-"符号，将其与父错误的错误信息连接起来，形成一个完整的错误信息ctx。
			if parentErr := err.(protocol.ChildErr).Parent(); parentErr != nil {
				ctx = " - " + parentErr.Error()
			}
			p.nsqd.logf(LOG_ERROR, "[%s] - %s%s", client, err, ctx)

			sendErr := p.Send(client, frameTypeError, []byte(err.Error()))
			if sendErr != nil { // 无法向客户端写入信息则强行关闭连接。
				p.nsqd.logf(LOG_ERROR, "[%s] - %s%s", client, sendErr, ctx)
				break
			}

			// FatalClientErr类型的错误应该强行关闭连接。
			if _, ok := err.(*protocol.FatalClientErr); ok {
				break
			}
			continue
		}

		if response != nil { // 存在返回结果时则向客户端写入流中写入信息
			err = p.Send(client, frameTypeResponse, response)
			if err != nil { // 无法向客户端写入信息则强行关闭连接。
				err = fmt.Errorf("failed to send response - %s", err)
				break
			}
		}
	}

	p.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] exiting ioloop", client)
	close(client.ExitChan)     // 通知tcp消息泵messagePump优雅退出服务
	if client.Channel != nil { // 如果订阅过频道则从此频道的客户端列表中删除指定客户端消费者对象信息
		client.Channel.RemoveClient(client.ID)
	}

	return err
}

func (p *protocolV2) SendMessage(client *clientV2, msg *Message) error {
	p.nsqd.logf(LOG_DEBUG, "PROTOCOL(V2): writing msg(%s) to client(%s) - %s", msg.ID, client, msg.Body)

	buf := bufferPoolGet()
	defer bufferPoolPut(buf)

	msg.deferred = 0
	_, err := msg.WriteTo(buf) // 将msg对象写入到buf对象中
	if err != nil {
		return err
	}

	err = p.Send(client, frameTypeMessage, buf.Bytes()) // 发送数据到客户端流中
	if err != nil {
		return err
	}

	return nil
}

func (p *protocolV2) Send(client *clientV2, frameType int32, data []byte) error {
	client.writeLock.Lock()

	var zeroTime time.Time
	if client.HeartbeatInterval > 0 {
		client.SetWriteDeadline(time.Now().Add(client.HeartbeatInterval))
	} else {
		client.SetWriteDeadline(zeroTime)
	}

	_, err := protocol.SendFramedResponse(client.Writer, frameType, data)
	if err != nil {
		client.writeLock.Unlock()
		return err
	}

	if frameType != frameTypeMessage {
		err = client.Flush()
	}

	client.writeLock.Unlock()

	return err
}

// Exec TCP路由逻辑
func (p *protocolV2) Exec(client *clientV2, params [][]byte) ([]byte, error) {
	if bytes.Equal(params[0], []byte("IDENTIFY")) {
		return p.IDENTIFY(client, params) // IDENTIFY 鉴权数据更新(消费者方法)
	}
	err := enforceTLSPolicy(client, p, params[0]) // enforceTLSPolicy 用于检查并强制执行 TLS 策略。
	if err != nil {
		return nil, err
	}
	switch {
	case bytes.Equal(params[0], []byte("FIN")):
		return p.FIN(client, params) // FIN 修改消息状态为完成消费状态(消费者方法)
	case bytes.Equal(params[0], []byte("RDY")):
		return p.RDY(client, params) // RDY 设置客户端的读取数据量条数(消费者方法)
	case bytes.Equal(params[0], []byte("REQ")):
		return p.REQ(client, params) // REQ 发送请求将指定消息ID重新加入到消费队列中(消费者方法)
	case bytes.Equal(params[0], []byte("PUB")):
		return p.PUB(client, params) // PUB 发布单条消息(生产者方法)
	case bytes.Equal(params[0], []byte("MPUB")):
		return p.MPUB(client, params) // MPUB 发布多条消息(生产者方法)
	case bytes.Equal(params[0], []byte("DPUB")):
		return p.DPUB(client, params) // DPUB 发布一条延迟消息(生产者方法)
	case bytes.Equal(params[0], []byte("NOP")):
		return p.NOP(client, params) // NOP 空操作,啥也不做
	case bytes.Equal(params[0], []byte("TOUCH")):
		return p.TOUCH(client, params) // TOUCH 重置飞行中(消费中)消息的超时设置(消费者方法)
	case bytes.Equal(params[0], []byte("SUB")):
		return p.SUB(client, params) // SUB 消费者订阅指定topic并创建channel对象(消费者方法)
	case bytes.Equal(params[0], []byte("CLS")):
		return p.CLS(client, params) // CLS 客户端连接请求关闭(消费者方法)
	case bytes.Equal(params[0], []byte("AUTH")):
		return p.AUTH(client, params) // AUTH 客户端连接认证方法(消费者方法)
	}
	return nil, protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

// messagePump 客户端连接对象订阅指定topic下的channel后,此方法会周期性的管理是否需要将消息发送到此客户端流中
func (p *protocolV2) messagePump(client *clientV2, startedChan chan bool) {
	var err error
	var memoryMsgChan chan *Message
	var backendMsgChan <-chan []byte // 仅可读权限的通道
	var subChannel *Channel
	// flusherChan 周期刷新客户端写入流的可读通道(outputBufferTicker定时器提供)
	var flusherChan <-chan time.Time // 仅可读权限的通道
	var sampleRate int32

	subEventChan := client.SubEventChan                              // 将子事件频道对应的通道对象赋值给subEventChan对象
	identifyEventChan := client.IdentifyEventChan                    // 将鉴权事件通道对象赋值给identifyEventChan对象
	outputBufferTicker := time.NewTicker(client.OutputBufferTimeout) // 根据输出刷新频率生成定时器(默认频率250毫秒)
	heartbeatTicker := time.NewTicker(client.HeartbeatInterval)      // 根据心跳检测频率生成定时器(默认频率客户端连接超时频率60秒/2)
	heartbeatChan := heartbeatTicker.C                               // 赋值心跳定时器的可读通道给heartbeatChan对象
	msgTimeout := client.MsgTimeout                                  // 消息被客户端读取后等待的最长时间，过了此时间则自动重新入队，默认1分钟

	// v2会周期性地将数据缓冲给客户端，以减少写系统的调用。
	// 我们在两种情况下强制冲刷：
	// 1.当客户端没有准备好接收信息时
	// 2.我们被缓冲了，通道没有什么东西可以发给我们了（也就是说，无论如何我们都会在这个循环中阻塞）。
	//
	flushed := true

	// 向启动messagePump的goroutine发出信号，表示我们已经启动了
	close(startedChan)

	for {
		if subChannel == nil || !client.IsReadyForMessages() {
			// 客户端未开启订阅或订阅的通道暂停分发任务时将执行下面的逻辑
			memoryMsgChan = nil
			backendMsgChan = nil
			flusherChan = nil
			// 强制客户端连接的刷新
			client.writeLock.Lock()
			err = client.Flush()
			client.writeLock.Unlock()
			if err != nil {
				goto exit
			}
			flushed = true
		} else if flushed {
			// 刷新状态下,重新更新内存通道对象和磁盘队列对象
			memoryMsgChan = subChannel.memoryMsgChan
			backendMsgChan = subChannel.backend.ReadChan()
			flusherChan = nil
		} else {
			// 客户端已经准备号且非刷新的情况下,重新更新内存通道对象和磁盘队列对象并设置定期刷新客户端流的通道对象
			memoryMsgChan = subChannel.memoryMsgChan
			backendMsgChan = subChannel.backend.ReadChan()
			flusherChan = outputBufferTicker.C
		}

		select {
		case <-flusherChan: // 收到刷新写入流通知 (注意:鉴权数据若未设置则定时器通道将永远关闭)
			client.writeLock.Lock()
			err = client.Flush()
			client.writeLock.Unlock()
			if err != nil { // 刷新数据异常走退出流程
				goto exit
			}
			flushed = true
		case <-client.ReadyStateChan: // 接收到频道channel的开启或关闭通知,重新跳转到上面的逻辑中检查通道是否暂停分发任务
		case subChannel = <-subEventChan: // 获取订阅topic下的channel对象(客户端连接后第二步是订阅)
			// 订阅成功后不再重新获取订阅的channel对象
			subEventChan = nil
		case identifyData := <-identifyEventChan: // 获取客户端连接的鉴权数据(客户端连接后第一步是鉴权)
			// 鉴权数据获得后不再重新获取鉴权数据信息
			identifyEventChan = nil

			outputBufferTicker.Stop()                 // 关闭默认的输出流定时器,根据鉴权数据设置
			if identifyData.OutputBufferTimeout > 0 { // 如果鉴权数据中设置了输出刷新频率则重新生成输出流定时器
				outputBufferTicker = time.NewTicker(identifyData.OutputBufferTimeout)
			}

			heartbeatTicker.Stop()                  // 关闭默认的心跳定时器,根据鉴权数据设置
			heartbeatChan = nil                     // 清空心跳可读通道对象
			if identifyData.HeartbeatInterval > 0 { // 如果鉴权数据中设置了心跳监测频率则重新生成心跳定时器和心跳可读通道对象
				heartbeatTicker = time.NewTicker(identifyData.HeartbeatInterval)
				heartbeatChan = heartbeatTicker.C
			}

			if identifyData.SampleRate > 0 { // 如果鉴权数据中设置了采样率则更新采样率
				sampleRate = identifyData.SampleRate
			}

			msgTimeout = identifyData.MsgTimeout // 设置此客户端连接的消息消费最长时间
		case <-heartbeatChan: // 到达心跳通知时间(注意:鉴权数据若未设置则定时器通道将永远关闭)
			err = p.Send(client, frameTypeResponse, heartbeatBytes) // 发送心跳通知到客户端上
			if err != nil {
				goto exit
			}
		case b := <-backendMsgChan: // 磁盘队列中获取到数据
			// 采样率存在且随机值在采样率范围内则仅处理随机的这部分数据，若为0则处理读取的所有数据
			if sampleRate > 0 && rand.Int31n(100) > sampleRate {
				continue
			}

			msg, err := decodeMessage(b) // 解析出消息对象
			if err != nil {
				p.nsqd.logf(LOG_ERROR, "failed to decode message - %s", err)
				continue
			}
			if msg.deferred != 0 {
				subChannel.StartDeferredTimeout(msg, msg.deferred)
				continue
			}

			msg.Attempts++ // 消息对象发送到消费队列中前需要设置此消息超时时间并增加一次分发次数

			subChannel.StartInFlightTimeout(msg, client.ID, msgTimeout) // 设置消费截至时间
			client.SendingMessage()                                     // 更新消费队列中的计数和总消费量计数
			err = p.SendMessage(client, msg)                            // 发送消息对象到client的写入流中
			if err != nil {
				goto exit
			}
			flushed = false
		case msg := <-memoryMsgChan: // 内存通道中获取到数据
			if sampleRate > 0 && rand.Int31n(100) > sampleRate {
				continue
			}
			msg.Attempts++ // 消息对象发送到消费队列中前需要设置此消息超时时间并增加一次分发次数

			subChannel.StartInFlightTimeout(msg, client.ID, msgTimeout) // 设置消费截至时间
			client.SendingMessage()                                     // 更新消费队列中的计数和总消费量计数
			err = p.SendMessage(client, msg)                            // 发送消息对象到client的写入流中
			if err != nil {
				goto exit
			}
			flushed = false
		case <-client.ExitChan:
			goto exit
		}
	}

exit:
	p.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] exiting messagePump", client)
	heartbeatTicker.Stop()    // 关闭心跳定时器
	outputBufferTicker.Stop() // 关闭输出流定时器
	if err != nil {
		p.nsqd.logf(LOG_ERROR, "PROTOCOL(V2): [%s] messagePump error - %s", client, err)
	}
}

// IDENTIFY 鉴权数据更新(消费者方法)
// 此命令是客户端作为消费者和nsqd之间建立连接后必须发送的第一个命令，只有成功鉴权才能执行后续消费的请求
func (p *protocolV2) IDENTIFY(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	if atomic.LoadInt32(&client.State) != stateInit { // 客户端状态不是初始状态则返回错误信息
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot IDENTIFY in current state")
	}

	bodyLen, err := readLen(client.Reader, client.lenSlice) // 从读取流client.Reader中的前4字节解析并获取内容长度
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}

	if int64(bodyLen) > p.nsqd.getOpts().MaxBodySize { // 数据内容长度大于接收消息的最大长度(默认5MB)则返回错误信息
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY body too big %d > %d", bodyLen, p.nsqd.getOpts().MaxBodySize))
	}

	if bodyLen <= 0 { // 没有数据内容则返回错误信息
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY invalid body size %d", bodyLen))
	}

	body := make([]byte, bodyLen)             // 初始化内容长度大小的body对象用于接收读取流中的内容
	_, err = io.ReadFull(client.Reader, body) // 从读取流client.Reader中获取数据到body中
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	// body是客户端连接用来设置生产者信息的json数据
	var identifyData identifyDataV2
	err = json.Unmarshal(body, &identifyData)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	p.nsqd.logf(LOG_DEBUG, "PROTOCOL(V2): [%s] %+v", client, identifyData)

	err = client.Identify(identifyData)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY "+err.Error())
	}

	// 鉴权成功后判断客户端是否需要协商功能，如果不协商就提前退出
	if !identifyData.FeatureNegotiation {
		return okBytes, nil
	}

	tlsv1 := p.nsqd.tlsConfig != nil && identifyData.TLSv1             // 判断是否使用了tls的v1版本
	deflate := p.nsqd.getOpts().DeflateEnabled && identifyData.Deflate // 是否启用客户端Deflate的压缩算法
	deflateLevel := 6                                                  // 默认压缩等级为6
	if deflate && identifyData.DeflateLevel > 0 {                      // 存在压缩等级设置时,使用客户端设置压缩等级
		deflateLevel = identifyData.DeflateLevel
	}
	if max := p.nsqd.getOpts().MaxDeflateLevel; max < deflateLevel { // 压缩等级取值范围控制在服务端支持的最大压缩等级范围内
		deflateLevel = max
	}
	snappy := p.nsqd.getOpts().SnappyEnabled && identifyData.Snappy // 是否启用客户端Snappy的压缩算法

	if deflate && snappy { // 如果客户端同时设置了两种压缩方式则返回错误信息,不能同时启用
		return nil, protocol.NewFatalClientErr(nil, "E_IDENTIFY_FAILED", "cannot enable both deflate and snappy compression")
	}

	resp, err := json.Marshal(struct {
		MaxRdyCount         int64  `json:"max_rdy_count"`
		Version             string `json:"version"`
		MaxMsgTimeout       int64  `json:"max_msg_timeout"`
		MsgTimeout          int64  `json:"msg_timeout"`
		TLSv1               bool   `json:"tls_v1"`
		Deflate             bool   `json:"deflate"`
		DeflateLevel        int    `json:"deflate_level"`
		MaxDeflateLevel     int    `json:"max_deflate_level"`
		Snappy              bool   `json:"snappy"`
		SampleRate          int32  `json:"sample_rate"`
		AuthRequired        bool   `json:"auth_required"`
		OutputBufferSize    int    `json:"output_buffer_size"`
		OutputBufferTimeout int64  `json:"output_buffer_timeout"`
	}{
		MaxRdyCount:         p.nsqd.getOpts().MaxRdyCount,
		Version:             version.Binary,
		MaxMsgTimeout:       int64(p.nsqd.getOpts().MaxMsgTimeout / time.Millisecond),
		MsgTimeout:          int64(client.MsgTimeout / time.Millisecond),
		TLSv1:               tlsv1,
		Deflate:             deflate,
		DeflateLevel:        deflateLevel,
		MaxDeflateLevel:     p.nsqd.getOpts().MaxDeflateLevel,
		Snappy:              snappy,
		SampleRate:          client.SampleRate,
		AuthRequired:        p.nsqd.IsAuthEnabled(),
		OutputBufferSize:    client.OutputBufferSize,
		OutputBufferTimeout: int64(client.OutputBufferTimeout / time.Millisecond),
	}) // 返回nsqd服务端的配置信息
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	err = p.Send(client, frameTypeResponse, resp) // 将数据写入到写入流中
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	if tlsv1 { // 客户端如果启用了tls协议则执行下面逻辑
		p.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] upgrading connection to TLS", client)
		err = client.UpgradeTLS()
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	if snappy {
		p.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] upgrading connection to snappy", client)
		err = client.UpgradeSnappy()
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	if deflate {
		p.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] upgrading connection to deflate (level %d)", client, deflateLevel)
		err = client.UpgradeDeflate(deflateLevel)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	return nil, nil
}

// AUTH 客户端连接认证方法(消费者方法)
// IDENTIFY方法后SUB方法前执行
func (p *protocolV2) AUTH(client *clientV2, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot AUTH in current state")
	}

	if len(params) != 1 { // 请求数据列表参数数量异常直接退出
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "AUTH invalid number of parameters")
	}

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "AUTH failed to read body size")
	}

	if int64(bodyLen) > p.nsqd.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("AUTH body too big %d > %d", bodyLen, p.nsqd.getOpts().MaxBodySize))
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("AUTH invalid body size %d", bodyLen))
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "AUTH failed to read body")
	}

	if client.HasAuthorizations() {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "AUTH already set")
	}

	if !client.nsqd.IsAuthEnabled() {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_DISABLED", "AUTH disabled")
	}

	if err := client.Auth(string(body)); err != nil {
		// we don't want to leak errors contacting the auth server to untrusted clients
		p.nsqd.logf(LOG_WARN, "PROTOCOL(V2): [%s] AUTH failed %s", client, err)
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_FAILED", "AUTH failed")
	}

	if !client.HasAuthorizations() {
		return nil, protocol.NewFatalClientErr(nil, "E_UNAUTHORIZED", "AUTH no authorizations found")
	}

	resp, err := json.Marshal(struct {
		Identity        string `json:"identity"`
		IdentityURL     string `json:"identity_url"`
		PermissionCount int    `json:"permission_count"`
	}{
		Identity:        client.AuthState.Identity,
		IdentityURL:     client.AuthState.IdentityURL,
		PermissionCount: len(client.AuthState.Authorizations),
	})
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_ERROR", "AUTH error "+err.Error())
	}

	err = p.Send(client, frameTypeResponse, resp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_ERROR", "AUTH error "+err.Error())
	}

	return nil, nil

}

// CheckAuth 检查客户端连接是否具有针对特定主题和频道的授权
// 首先判断当前主题和频道是否需要授权（即启用了授权功能）。
// 如果启用了授权功能，但客户端没有提供有效的授权，则返回一个[E_AUTH_FIRST]的错误消息，提示客户端必须先进行授权，才能继续执行后续操作。
// 接着，客户端的授权信息会被重新验证，如果存在错误，则返回一个[E_AUTH_FAILED]的错误消息。
// 最后，如果客户端的授权有效，则返回 nil，表示授权成功。
// 如果客户端授权无效，则返回一个[E_UNAUTHORIZED]的错误消息，提示客户端没有权限执行相应的操作。
func (p *protocolV2) CheckAuth(client *clientV2, cmd, topicName, channelName string) error {
	if client.nsqd.IsAuthEnabled() {
		if !client.HasAuthorizations() { // 如果没有授权则返回错误信息
			return protocol.NewFatalClientErr(nil, "E_AUTH_FIRST",
				fmt.Sprintf("AUTH required before %s", cmd))
		}
		ok, err := client.IsAuthorized(topicName, channelName) // 重新授权topic和channel
		if err != nil {
			// 客户端和服务端授权异常将返回错误信息
			p.nsqd.logf(LOG_WARN, "PROTOCOL(V2): [%s] AUTH failed %s", client, err)
			return protocol.NewFatalClientErr(nil, "E_AUTH_FAILED", "AUTH failed")
		}
		if !ok { // 返回无权限的信息
			return protocol.NewFatalClientErr(nil, "E_UNAUTHORIZED",
				fmt.Sprintf("AUTH failed for %s on %q %q", cmd, topicName, channelName))
		}
	}
	return nil
}

// SUB 消费者订阅指定topic并创建channel对象(消费者方法)
func (p *protocolV2) SUB(client *clientV2, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != stateInit { // 客户端连接的状态如果是初始化状态则返回错误信息
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot SUB in current state")
	}

	if client.HeartbeatInterval <= 0 { // 客户端连接未设置心跳间隔时间则返回错误信息
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot SUB with heartbeats disabled")
	}

	if len(params) < 3 { // 请求数据列表参数缺失直接退出
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "SUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) { // 订阅的topic名称是无效的将返回错误信息
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("SUB topic name %q is not valid", topicName))
	}

	channelName := string(params[2])
	if !protocol.IsValidChannelName(channelName) { // 订阅的channel名称是无效的将返回错误信息
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_CHANNEL",
			fmt.Sprintf("SUB channel name %q is not valid", channelName))
	}

	if err := p.CheckAuth(client, "SUB", topicName, channelName); err != nil { // 判断是否需要授权,如果需要授权则检验权限是否正常,若权限过期则检验是否可以再次获取授权
		return nil, err
	}

	// 此重试循环是针对竞争条件的解决方案，其中最后一个客户端可以离开GetChannel()和AddClient()之间的通道。避免将客户端添加到已开始退出的临时频道/主题中。
	var channel *Channel
	for i := 1; ; i++ {
		topic := p.nsqd.GetTopic(topicName)                          // 获取指定topic对象(不存在则创建)
		channel = topic.GetChannel(channelName)                      // 获取指定此topic下的channel对象(不存在则创建)
		if err := channel.AddClient(client.ID, client); err != nil { // 将此客户端连接对象添加到频道channel的客户端列表中
			return nil, protocol.NewFatalClientErr(err, "E_SUB_FAILED", "SUB failed "+err.Error())
		}

		// 如果订阅的channel和topic是临时的并且是退出的则将此客户端连接对象从频道channel的客户端列表中移除,并重试两次
		if (channel.ephemeral && channel.Exiting()) || (topic.ephemeral && topic.Exiting()) {
			channel.RemoveClient(client.ID)
			if i < 2 { // 订阅临时的频道channel可以重试2次；每次间隔100毫秒
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return nil, protocol.NewFatalClientErr(nil, "E_SUB_FAILED", "SUB failed to deleted topic/channel")
		}
		break
	}
	atomic.StoreInt32(&client.State, stateSubscribed) // 订阅成功后修改此客户端连接状态为订阅状态
	client.Channel = channel                          // 初始化设置此客户端连接的channel对象
	// 更新消息泵,此客户端连接对象开始接收消息流
	client.SubEventChan <- channel

	return okBytes, nil
}

// RDY 设置客户端的读取数据量条数(消费者方法)
func (p *protocolV2) RDY(client *clientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)

	if state == stateClosing { // 客户端连接状态不能是关闭状态
		// 忽略关闭频道上的更改
		p.nsqd.logf(LOG_INFO,
			"PROTOCOL(V2): [%s] ignoring RDY after CLS in state ClientStateV2Closing",
			client)
		return nil, nil
	}

	if state != stateSubscribed {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot RDY in current state")
	}

	count := int64(1)    // 默认开启条数为1
	if len(params) > 1 { // 请求数据列表若设置则更新条数为设置值
		b10, err := protocol.ByteToBase10(params[1])
		if err != nil { // 参数内容异常直接退出
			return nil, protocol.NewFatalClientErr(err, "E_INVALID",
				fmt.Sprintf("RDY could not parse count %s", params[1]))
		}
		count = int64(b10)
	}

	if count < 0 || count > p.nsqd.getOpts().MaxRdyCount { // 判断设置条数是否合理,不能超过nsqd服务设置的最大值(默认2500条)
		// 这需要是一个致命的错误，直接返回错误信息
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("RDY count %d out of range 0-%d", count, p.nsqd.getOpts().MaxRdyCount))
	}

	client.SetReadyCount(count)

	return nil, nil
}

// FIN 修改消息状态为完成消费状态(消费者方法)
func (p *protocolV2) FIN(client *clientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing { // 客户端连接状态必须是订阅或关闭状态
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot FIN in current state")
	}

	if len(params) < 2 { // 请求数据列表参数缺失直接退出
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "FIN insufficient number of params")
	}

	id, err := getMessageID(params[1]) // 验证读取流中取出的参数并将其强制转换为消息ID
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", err.Error())
	}

	err = client.Channel.FinishMessage(client.ID, *id)
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_FIN_FAILED",
			fmt.Sprintf("FIN %s failed %s", *id, err.Error()))
	}

	client.FinishedMessage()

	return nil, nil
}

// REQ 发送请求将指定消息ID重新加入到消费队列中(消费者方法)
func (p *protocolV2) REQ(client *clientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)               // 获取客户端连接状态
	if state != stateSubscribed && state != stateClosing { // 客户端如果不是已订阅和关闭状态则直接退出
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot REQ in current state")
	}

	if len(params) < 3 { // 请求数据列表参数缺失直接退出
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "REQ insufficient number of params")
	}

	id, err := getMessageID(params[1]) // 验证读取流中取出的参数并将其强制转换为消息ID
	if err != nil {                    // 强制转换失败则直接退出
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", err.Error())
	}

	timeoutMs, err := protocol.ByteToBase10(params[2]) // 获取需要延迟的时间(毫秒)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_INVALID",
			fmt.Sprintf("REQ could not parse timeout %s", params[2]))
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	maxReqTimeout := p.nsqd.getOpts().MaxReqTimeout // 默认最长设置延迟时间1小时
	clampedTimeout := timeoutDuration

	if timeoutDuration < 0 {
		clampedTimeout = 0
	} else if timeoutDuration > maxReqTimeout {
		clampedTimeout = maxReqTimeout
	}
	if clampedTimeout != timeoutDuration { // 控制延迟时间clampedTimeout后与设置延迟时间不等时打印日志输出
		p.nsqd.logf(LOG_INFO, "PROTOCOL(V2): [%s] REQ timeout %d out of range 0-%d. Setting to %d",
			client, timeoutDuration, maxReqTimeout, clampedTimeout)
		timeoutDuration = clampedTimeout
	}

	err = client.Channel.RequeueMessage(client.ID, *id, timeoutDuration) // 向通道提交需要重新入消费队列的任务
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_REQ_FAILED",
			fmt.Sprintf("REQ %s failed %s", *id, err.Error()))
	}

	client.RequeuedMessage() // 计数统计更新

	return nil, nil
}

// CLS 客户端连接请求关闭(消费者方法)
func (p *protocolV2) CLS(client *clientV2, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != stateSubscribed { // 订阅状态下才能关闭客户端连接
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot CLS in current state")
	}

	client.StartClose()

	return []byte("CLOSE_WAIT"), nil
}

// NOP 空操作,啥也不做
func (p *protocolV2) NOP(client *clientV2, params [][]byte) ([]byte, error) {
	return nil, nil
}

// PUB 发布单条消息(生产者方法)
func (p *protocolV2) PUB(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	if len(params) < 2 { // 请求数据列表参数缺失直接退出
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "PUB insufficient number of parameters")
	}

	// 获取检验请求参数的topic名称是否合法
	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("PUB topic name %q is not valid", topicName))
	}

	// 读取前4字节获取请求体长度
	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "PUB failed to read message body size")
	}

	if bodyLen <= 0 { // 请求体长度异常直接退出
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("PUB invalid message body size %d", bodyLen))
	}

	if int64(bodyLen) > p.nsqd.getOpts().MaxMsgSize { // 请求体长度超过nsqd服务设置最大值直接退出
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("PUB message too big %d > %d", bodyLen, p.nsqd.getOpts().MaxMsgSize))
	}

	// 读取请求体中的内容
	messageBody := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, messageBody)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "PUB failed to read message body")
	}

	// 检查客户端连接是否具有针对特定主题和频道的授权
	if err := p.CheckAuth(client, "PUB", topicName, ""); err != nil {
		return nil, err
	}

	topic := p.nsqd.GetTopic(topicName)
	msg := NewMessage(topic.GenerateID(), messageBody)
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_PUB_FAILED", "PUB failed "+err.Error())
	}

	client.PublishedMessage(topicName, 1)

	return okBytes, nil
}

// MPUB 发布多条消息(生产者方法)
func (p *protocolV2) MPUB(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	if len(params) < 2 { // 请求数据列表参数缺失直接退出
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "MPUB insufficient number of parameters")
	}

	// 获取检验请求参数的topic名称是否合法
	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("E_BAD_TOPIC MPUB topic name %q is not valid", topicName))
	}

	// 检查客户端连接是否具有针对特定主题和频道的授权
	if err := p.CheckAuth(client, "MPUB", topicName, ""); err != nil {
		return nil, err
	}

	topic := p.nsqd.GetTopic(topicName)

	// 读取前4字节获取请求体长度
	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "MPUB failed to read body size")
	}

	if bodyLen <= 0 { // 请求体长度异常直接退出
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("MPUB invalid body size %d", bodyLen))
	}

	if int64(bodyLen) > p.nsqd.getOpts().MaxBodySize { // 请求体长度超过nsqd服务设置最大值直接退出
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("MPUB body too big %d > %d", bodyLen, p.nsqd.getOpts().MaxBodySize))
	}

	messages, err := readMPUB(client.Reader, client.lenSlice, topic,
		p.nsqd.getOpts().MaxMsgSize, p.nsqd.getOpts().MaxBodySize)
	if err != nil {
		return nil, err
	}

	// 如果我们走到这一步，我们已经验证了所有的输入，唯一可能的错误是主题在接下来的调用中退出（在这种情况下没有消息会被排队）。
	err = topic.PutMessages(messages)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_MPUB_FAILED", "MPUB failed "+err.Error())
	}

	client.PublishedMessage(topicName, uint64(len(messages)))

	return okBytes, nil
}

// DPUB 发布一条延迟消息(生产者方法)
func (p *protocolV2) DPUB(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	if len(params) < 3 { // 请求数据列表参数缺失直接退出
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "DPUB insufficient number of parameters")
	}

	// 获取检验请求参数的topic名称是否合法
	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("DPUB topic name %q is not valid", topicName))
	}

	// 获取检验请求参数的延迟时间(单位:ms)是否合法
	timeoutMs, err := protocol.ByteToBase10(params[2])
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_INVALID",
			fmt.Sprintf("DPUB could not parse timeout %s", params[2]))
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	// 延迟时间超过nsqd规定范围[0,3600](单位:s)则直接返回错误信息
	if timeoutDuration < 0 || timeoutDuration > p.nsqd.getOpts().MaxReqTimeout {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("DPUB timeout %d out of range 0-%d",
				timeoutMs, p.nsqd.getOpts().MaxReqTimeout/time.Millisecond))
	}

	// 读取前4字节获取请求体长度
	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "DPUB failed to read message body size")
	}

	if bodyLen <= 0 { // 请求体长度异常直接退出
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("DPUB invalid message body size %d", bodyLen))
	}

	if int64(bodyLen) > p.nsqd.getOpts().MaxMsgSize { // 请求体长度超过nsqd服务设置最大值直接退出
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("DPUB message too big %d > %d", bodyLen, p.nsqd.getOpts().MaxMsgSize))
	}

	messageBody := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, messageBody)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "DPUB failed to read message body")
	}

	// 检查客户端连接是否具有针对特定主题和频道的授权
	if err := p.CheckAuth(client, "DPUB", topicName, ""); err != nil {
		return nil, err
	}

	// 获取topic对象,生成消息对象并设置延迟时间然后发送到topic的消息队列中
	topic := p.nsqd.GetTopic(topicName)
	msg := NewMessage(topic.GenerateID(), messageBody)
	msg.deferred = timeoutDuration
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_DPUB_FAILED", "DPUB failed "+err.Error())
	}

	client.PublishedMessage(topicName, 1)

	return okBytes, nil
}

// TOUCH 重置飞行中(消费中)消息的超时设置(消费者方法)
func (p *protocolV2) TOUCH(client *clientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing { // 客户端如果不是已订阅和关闭状态则直接退出
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot TOUCH in current state")
	}

	if len(params) < 2 { // 请求数据列表参数缺失直接退出
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "TOUCH insufficient number of params")
	}

	id, err := getMessageID(params[1]) // 验证读取流中取出的参数并将其强制转换为消息ID
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", err.Error())
	}

	client.writeLock.RLock()
	msgTimeout := client.MsgTimeout
	client.writeLock.RUnlock()
	err = client.Channel.TouchMessage(client.ID, *id, msgTimeout) // 重置飞行中消息的超时设置
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_TOUCH_FAILED",
			fmt.Sprintf("TOUCH %s failed %s", *id, err.Error()))
	}

	return nil, nil
}

// readMPUB 从读取流r中读取所有的消息体对象
func readMPUB(r io.Reader, tmp []byte, topic *Topic, maxMessageSize int64, maxBodySize int64) ([]*Message, error) {
	numMessages, err := readLen(r, tmp) // 从读取流r中的前4字节解析并获取消息体的个数
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "MPUB failed to read message count")
	}

	// 4 == total num, 5 == length + min 1
	maxMessages := (maxBodySize - 4) / 5
	if numMessages <= 0 || int64(numMessages) > maxMessages { // 控制消息体的个数范围[1, (max-body-size-4)/5]
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY",
			fmt.Sprintf("MPUB invalid message count %d", numMessages))
	}

	messages := make([]*Message, 0, numMessages)
	for i := int32(0); i < numMessages; i++ { // 读取所有消息体
		messageSize, err := readLen(r, tmp) // 消息体的前4字节(消息体长度)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB failed to read message(%d) body size", i))
		}

		if messageSize <= 0 { // 消息体长度异常直接退出
			return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB invalid message(%d) body size %d", i, messageSize))
		}

		if int64(messageSize) > maxMessageSize { // 消息体长度超过nsqd服务设置最大消息体大小直接退出
			return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB message too big %d > %d", messageSize, maxMessageSize))
		}

		msgBody := make([]byte, messageSize)
		_, err = io.ReadFull(r, msgBody)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "MPUB failed to read message body")
		}

		messages = append(messages, NewMessage(topic.GenerateID(), msgBody)) // 生成每个消息对象
	}

	return messages, nil
}

// 验证读取流中取出的参数并将其强制转换为消息ID
func getMessageID(p []byte) (*MessageID, error) {
	if len(p) != MsgIDLength {
		return nil, errors.New("invalid message ID")
	}
	return (*MessageID)(unsafe.Pointer(&p[0])), nil
}

// readLen 读取内容数据存放到tmp变量中再转换成长度数据返回
func readLen(r io.Reader, tmp []byte) (int32, error) {
	_, err := io.ReadFull(r, tmp)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(tmp)), nil
}

// enforceTLSPolicy 用于检查并强制执行 TLS 策略。对于每个接收到的消息，都需要先进行 TLS 策略的检查。
// 如果客户端当前没有使用 TLS 进行加密通信，但是当前nsqd的 TLS 策略要求客户端必须使用 TLS，则不能处理这个消息。
func enforceTLSPolicy(client *clientV2, p *protocolV2, command []byte) error {
	if p.nsqd.getOpts().TLSRequired != TLSNotRequired && atomic.LoadInt32(&client.TLS) != 1 {
		return protocol.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("cannot %s in current state (TLS required)", command))
	}
	return nil
}
