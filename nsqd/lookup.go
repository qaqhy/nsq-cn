package nsqd

import (
	"bytes"
	"encoding/json"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/version"
)

// connectCallback 连接回调方法,集成服务发现交互的鉴权和nsqd信息的注册方法
func connectCallback(n *NSQD, hostname string) func(*lookupPeer) {
	return func(lp *lookupPeer) {
		ci := make(map[string]interface{})
		ci["version"] = version.Binary
		ci["tcp_port"] = n.getOpts().BroadcastTCPPort
		ci["http_port"] = n.getOpts().BroadcastHTTPPort
		ci["hostname"] = hostname
		ci["broadcast_address"] = n.getOpts().BroadcastAddress

		cmd, err := nsq.Identify(ci) // 初始化鉴权命令对象
		if err != nil {
			lp.Close()
			return
		}

		resp, err := lp.Command(cmd) // 开始鉴权同步信息
		if err != nil {
			n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lp, cmd, err)
			return
		} else if bytes.Equal(resp, []byte("E_INVALID")) { // 同步异常则关闭此服务发现客户端连接对象
			n.logf(LOG_INFO, "LOOKUPD(%s): lookupd returned %s", lp, resp)
			lp.Close()
			return
		}

		err = json.Unmarshal(resp, &lp.Info) // 同步服务端返回信息
		if err != nil {
			n.logf(LOG_ERROR, "LOOKUPD(%s): parsing response - %s", lp, resp)
			lp.Close()
			return
		}
		n.logf(LOG_INFO, "LOOKUPD(%s): peer info %+v", lp, lp.Info)
		if lp.Info.BroadcastAddress == "" { // 连接的服务发现未返回地址信息则打印错误信息
			n.logf(LOG_ERROR, "LOOKUPD(%s): no broadcast address", lp)
		}

		// 首先构建所有topic和channel的注册命令,以便我们尽可能快地退出锁
		var commands []*nsq.Command
		n.RLock()
		for _, topic := range n.topicMap {
			topic.RLock()
			if len(topic.channelMap) == 0 {
				commands = append(commands, nsq.Register(topic.name, ""))
			} else {
				for _, channel := range topic.channelMap {
					commands = append(commands, nsq.Register(channel.topicName, channel.name))
				}
			}
			topic.RUnlock()
		}
		n.RUnlock()

		for _, cmd := range commands { // 执行所有的注册命令
			n.logf(LOG_INFO, "LOOKUPD(%s): %s", lp, cmd)
			_, err := lp.Command(cmd)
			if err != nil {
				n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lp, cmd, err)
				return
			}
		}
	}
}

// lookupLoop nsq的服务发现同步
func (n *NSQD) lookupLoop() {
	var lookupPeers []*lookupPeer // 服务发现服务器列表
	var lookupAddrs []string      // 服务发现服务器地址列表
	connect := true

	hostname, err := os.Hostname()
	if err != nil {
		n.logf(LOG_FATAL, "failed to get hostname - %s", err)
		os.Exit(1)
	}

	// for announcements, lookupd determines the host automatically
	ticker := time.NewTicker(15 * time.Second) // 初始化心跳定时器,周期（15秒）发送心跳信息
	defer ticker.Stop()
	for {
		if connect {
			for _, host := range n.getOpts().NSQLookupdTCPAddresses {
				if in(host, lookupAddrs) { // 存在老列表中的忽略
					continue
				}
				n.logf(LOG_INFO, "LOOKUP(%s): adding peer", host)
				lookupPeer := newLookupPeer(host, n.getOpts().MaxBodySize, n.logf,
					connectCallback(n, hostname))
				lookupPeer.Command(nil) // 执行空命令,如果非连接状态则开始Identify鉴权并修改为连接状态,然后同步此nsqd服务上所有的topic和channel信息到此服务发现上
				lookupPeers = append(lookupPeers, lookupPeer)
				lookupAddrs = append(lookupAddrs, host)
			}
			n.lookupPeers.Store(lookupPeers) // 更新lookupPeers服务发现列表到原子数据lookupPeers中
			connect = false
		}

		select {
		case <-ticker.C:
			// 发送心跳并读取响应（读取检测到关闭的连接）
			for _, lookupPeer := range lookupPeers {
				n.logf(LOG_DEBUG, "LOOKUPD(%s): sending heartbeat", lookupPeer)
				cmd := nsq.Ping()
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lookupPeer, cmd, err)
				}
			}
		case val := <-n.notifyChan: // 收到topic或channel的变动信号,执行下面的逻辑
			var cmd *nsq.Command
			var branch string

			switch val := val.(type) {
			case *Channel:
				// 有一个channel新创建或者被删除
				branch = "channel"
				channel := val
				if channel.Exiting() { // 此channel已退出则生成注销命令对象
					cmd = nsq.UnRegister(channel.topicName, channel.name)
				} else {
					cmd = nsq.Register(channel.topicName, channel.name)
				}
			case *Topic:
				// 有一个topic新创建或者被删除
				branch = "topic"
				topic := val
				if topic.Exiting() { // 此topic已退出则生成注销命令对象
					cmd = nsq.UnRegister(topic.name, "")
				} else {
					cmd = nsq.Register(topic.name, "")
				}
			}

			for _, lookupPeer := range lookupPeers { // 通知所有的nsqlookupds服务执行注册或注销命令
				n.logf(LOG_INFO, "LOOKUPD(%s): %s %s", lookupPeer, branch, cmd)
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					n.logf(LOG_ERROR, "LOOKUPD(%s): %s - %s", lookupPeer, cmd, err)
				}
			}
		case <-n.optsNotificationChan: // 收到服务发现列表数据更新通知执行下面逻辑
			var tmpPeers []*lookupPeer
			var tmpAddrs []string
			for _, lp := range lookupPeers { // 老列表中若存在则直接加入到新列表中
				if in(lp.addr, n.getOpts().NSQLookupdTCPAddresses) {
					tmpPeers = append(tmpPeers, lp)
					tmpAddrs = append(tmpAddrs, lp.addr)
					continue
				}
				n.logf(LOG_INFO, "LOOKUP(%s): removing peer", lp)
				lp.Close() // 删除的服务发现地址则关闭对应客户端对象
			}
			lookupPeers = tmpPeers
			lookupAddrs = tmpAddrs
			connect = true // 标记下次重新更新一次服务器对象列表
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	n.logf(LOG_INFO, "LOOKUP: closing")
}

func in(s string, lst []string) bool {
	for _, v := range lst {
		if s == v {
			return true
		}
	}
	return false
}

// lookupdHTTPAddrs 更新服务发现的所有HTTP地址列表(IPv4或IPv6)
func (n *NSQD) lookupdHTTPAddrs() []string {
	var lookupHTTPAddrs []string
	lookupPeers := n.lookupPeers.Load()
	if lookupPeers == nil {
		return nil
	}
	for _, lp := range lookupPeers.([]*lookupPeer) {
		if len(lp.Info.BroadcastAddress) <= 0 {
			continue
		}
		addr := net.JoinHostPort(lp.Info.BroadcastAddress, strconv.Itoa(lp.Info.HTTPPort)) // 拼接地址和端口信息(IPv4或IPv6)
		lookupHTTPAddrs = append(lookupHTTPAddrs, addr)
	}
	return lookupHTTPAddrs
}
