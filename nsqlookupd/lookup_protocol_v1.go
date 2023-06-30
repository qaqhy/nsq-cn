package nsqlookupd

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/version"
)

type LookupProtocolV1 struct {
	nsqlookupd *NSQLookupd
}

// NewClient 生成V1协议处理客户端
func (p *LookupProtocolV1) NewClient(conn net.Conn) protocol.Client {
	return NewClientV1(conn)
}

// IOLoop nsqlookupd TCP处理的核心逻辑
// 需要注意的是每个客户端连接都是单线程调用不能并发调用(客户端请求后必须等到服务端响应后才能做后续请求)
func (p *LookupProtocolV1) IOLoop(c protocol.Client) error {
	var err error
	var line string

	client := c.(*ClientV1)

	reader := bufio.NewReader(client) // 获取客户端对象的读取流对象
	for {
		line, err = reader.ReadString('\n') // 读取流中的一行数据,读到\n时结束
		if err != nil {                     // 若读取失败则此客户端连接异常,走后续的退出逻辑
			break
		}

		line = strings.TrimSpace(line)     // 去除首尾空格
		params := strings.Split(line, " ") // 以空格切割获取请求参数

		var response []byte
		response, err = p.Exec(client, reader, params) // 分发到路由上处理客户端请求内容
		if err != nil {
			ctx := ""
			// 首先将err强制类型转换为protocol.ChildErr类型。
			// 然后，从这个child error中获取到它的parentErr父错误，判断是否存在。
			// 如果存在父错误，则在错误信息前面添加一个"-"符号，将其与父错误的错误信息连接起来，形成一个完整的错误信息ctx。
			if parentErr := err.(protocol.ChildErr).Parent(); parentErr != nil {
				ctx = " - " + parentErr.Error()
			}
			p.nsqlookupd.logf(LOG_ERROR, "[%s] - %s%s", client, err, ctx)

			_, sendErr := protocol.SendResponse(client, []byte(err.Error()))
			if sendErr != nil { // 无法向客户端写入信息则强行关闭连接。
				p.nsqlookupd.logf(LOG_ERROR, "[%s] - %s%s", client, sendErr, ctx)
				break
			}

			// FatalClientErr类型的错误应该强行关闭连接。
			if _, ok := err.(*protocol.FatalClientErr); ok {
				break
			}
			continue
		}

		if response != nil { // 存在返回结果时则向客户端写入流中写入信息
			_, err = protocol.SendResponse(client, response)
			if err != nil { // 无法向客户端写入信息则强行关闭连接。
				break
			}
		}
	}

	p.nsqlookupd.logf(LOG_INFO, "PROTOCOL(V1): [%s] exiting ioloop", client)

	if client.peerInfo != nil { // 如果此客户端已存在注册信息,则在删除的同时清理掉对应的所有生产者信息
		registrations := p.nsqlookupd.DB.LookupRegistrations(client.peerInfo.id)
		for _, r := range registrations {
			if removed, _ := p.nsqlookupd.DB.RemoveProducer(r, client.peerInfo.id); removed { // 从注册关系DB的k字典中检查是否有成员id,若有则删除返回true,否则返回false
				p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
					client, r.Category, r.Key, r.SubKey)
			}
		}
	}

	return err
}

// Exec TCP路由逻辑
func (p *LookupProtocolV1) Exec(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	switch params[0] {
	case "PING":
		return p.PING(client, params) // PING 心跳通知,周期性调用(15秒)
	case "IDENTIFY":
		return p.IDENTIFY(client, reader, params[1:]) // IDENTIFY 鉴权信息同步
	case "REGISTER":
		return p.REGISTER(client, reader, params[1:]) // REGISTER 注册
	case "UNREGISTER":
		return p.UNREGISTER(client, reader, params[1:]) // UNREGISTER 注销
	}
	return nil, protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

// getTopicChan 获取参数中的topicName和channelName并检查是否合法
func getTopicChan(command string, params []string) (string, string, error) {
	if len(params) == 0 {
		return "", "", protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("%s insufficient number of params", command))
	}

	topicName := params[0]
	var channelName string
	if len(params) >= 2 {
		channelName = params[1]
	}

	if !protocol.IsValidTopicName(topicName) {
		return "", "", protocol.NewFatalClientErr(nil, "E_BAD_TOPIC", fmt.Sprintf("%s topic name '%s' is not valid", command, topicName))
	}

	if channelName != "" && !protocol.IsValidChannelName(channelName) {
		return "", "", protocol.NewFatalClientErr(nil, "E_BAD_CHANNEL", fmt.Sprintf("%s channel name '%s' is not valid", command, channelName))
	}

	return topicName, channelName, nil
}

// REGISTER 注册客户端连接对象
func (p *LookupProtocolV1) REGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.peerInfo == nil { // 如果此客户端连接对象未鉴权则返回错误信息(client must IDENTIFY)
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}

	topic, channel, err := getTopicChan("REGISTER", params) // 获取参数中的topicName和channelName并检查是否合法
	if err != nil {
		return nil, err
	}

	if channel != "" { // 存在channel,需要注册channel
		key := Registration{"channel", topic, channel}
		if p.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) { // 将生产者p添加到注册关系DB的k字典中并返回成员是否已存在于k字典中
			p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s",
				client, "channel", topic, channel)
		}
	}
	// 注册topic
	key := Registration{"topic", topic, ""}
	if p.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) { // 将生产者p添加到注册关系DB的k字典中并返回成员是否已存在于k字典中
		p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s",
			client, "topic", topic, "")
	}

	return []byte("OK"), nil
}

// UNREGISTER 注销客户端连接对象
func (p *LookupProtocolV1) UNREGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}

	topic, channel, err := getTopicChan("UNREGISTER", params) // 获取参数中的topicName和channelName并检查是否合法
	if err != nil {
		return nil, err
	}

	if channel != "" { // 存在channel,仅注销channel
		key := Registration{"channel", topic, channel}
		removed, left := p.nsqlookupd.DB.RemoveProducer(key, client.peerInfo.id) // 从注册关系DB的k字典中检查是否有成员id,若有则删除返回true,否则返回false
		if removed {                                                             // 服务发现中的生产者删除成功则打印输出日志信息
			p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
				client, "channel", topic, channel)
		}
		// 如果是临时频道,如果没有生产者就会删除该频道key
		if left == 0 && strings.HasSuffix(channel, "#ephemeral") {
			p.nsqlookupd.DB.RemoveRegistration(key) // 删除一个注册及其所有的生产者
		}
	} else {
		// 没有指定频道channel,所以这是一个取消注册的主题topic
		// 删除所有的频道channel注册...
		// 通常这不应该发生,这就是为什么我们要打印一个警告信息。
		registrations := p.nsqlookupd.DB.FindRegistrations("channel", topic, "*") // 查询所有符合条件的注册key列表
		for _, r := range registrations {
			removed, _ := p.nsqlookupd.DB.RemoveProducer(r, client.peerInfo.id) // 从注册关系DB的k字典中检查是否有成员id,若有则删除返回true,否则返回false
			if removed {
				p.nsqlookupd.logf(LOG_WARN, "client(%s) unexpected UNREGISTER category:%s key:%s subkey:%s",
					client, "channel", topic, r.SubKey)
			}
		}

		key := Registration{"topic", topic, ""}
		removed, left := p.nsqlookupd.DB.RemoveProducer(key, client.peerInfo.id) // 从注册关系DB的k字典中检查是否有成员id,若有则删除返回true,否则返回false
		if removed {                                                             // 服务发现中的生产者删除成功则打印输出日志信息
			p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
				client, "topic", topic, "")
		}
		// 如果是临时频道,如果没有生产者就会删除该频道key
		if left == 0 && strings.HasSuffix(topic, "#ephemeral") {
			p.nsqlookupd.DB.RemoveRegistration(key) // 删除一个注册及其所有的生产者
		}
	}

	return []byte("OK"), nil
}

// IDENTIFY 鉴权信息同步
func (p *LookupProtocolV1) IDENTIFY(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	var err error

	if client.peerInfo != nil {
		return nil, protocol.NewFatalClientErr(err, "E_INVALID", "cannot IDENTIFY again")
	}

	var bodyLen int32
	err = binary.Read(reader, binary.BigEndian, &bodyLen) // 大端方式读取消息的长度
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(reader, body) // 读取所有的数据到body对象中
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	// body是一个包含生产者信息的json结构,设置远程地址加端口为生产者id
	peerInfo := PeerInfo{id: client.RemoteAddr().String()}
	err = json.Unmarshal(body, &peerInfo) // 解析鉴权信息
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	peerInfo.RemoteAddress = client.RemoteAddr().String() // 设置远程地址加端口信息

	// 检查需要的字段是否都存在,若不存在则返回错误(鉴权失败)
	if peerInfo.BroadcastAddress == "" || peerInfo.TCPPort == 0 || peerInfo.HTTPPort == 0 || peerInfo.Version == "" {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY", "IDENTIFY missing fields")
	}

	atomic.StoreInt64(&peerInfo.lastUpdate, time.Now().UnixNano()) // 更新最近一次交换时间戳信息

	p.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): IDENTIFY Address:%s TCP:%d HTTP:%d Version:%s",
		client, peerInfo.BroadcastAddress, peerInfo.TCPPort, peerInfo.HTTPPort, peerInfo.Version) // 打印输出生产者的鉴权信息

	client.peerInfo = &peerInfo
	if p.nsqlookupd.DB.AddProducer(Registration{"client", "", ""}, &Producer{peerInfo: client.peerInfo}) { // 将生产者添加到注册关系DB中
		p.nsqlookupd.logf(LOG_INFO, "DB: client(%s) REGISTER category:%s key:%s subkey:%s", client, "client", "", "")
	}

	// 构建返回信息
	data := make(map[string]interface{})
	data["tcp_port"] = p.nsqlookupd.RealTCPAddr().Port   // 服务发现TCP端口号
	data["http_port"] = p.nsqlookupd.RealHTTPAddr().Port // 服务发现HTTP端口号
	data["version"] = version.Binary                     // 服务发现版本号
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err)
	}
	data["broadcast_address"] = p.nsqlookupd.opts.BroadcastAddress // 服务发现广播地址,这里是此服务发现的地址(命令行设置的broadcast-address参数)
	data["hostname"] = hostname                                    // 服务发现主机名

	response, err := json.Marshal(data)
	if err != nil {
		p.nsqlookupd.logf(LOG_ERROR, "marshaling %v", data) // json序列化失败则打印输出此错误信息
		return []byte("OK"), nil                            // 返回成功的标志信息
	}
	return response, nil
}

// PING 心跳通知,周期性调用(15秒)
func (p *LookupProtocolV1) PING(client *ClientV1, params []string) ([]byte, error) {
	if client.peerInfo != nil {
		// 我们可以在同一客户连接上的其他命令之前得到一个PING。
		cur := time.Unix(0, atomic.LoadInt64(&client.peerInfo.lastUpdate))
		now := time.Now()
		p.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): pinged (last ping %s)", client.peerInfo.id,
			now.Sub(cur)) // 打印输出最近两次心跳时间间隔
		atomic.StoreInt64(&client.peerInfo.lastUpdate, now.UnixNano()) // 更新此客户端连接的最近一次心跳时间戳信息
	}
	return []byte("OK"), nil
}
