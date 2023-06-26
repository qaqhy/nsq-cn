package nsqd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/protocol"
	"github.com/nsqio/nsq/internal/version"
)

var boolParams = map[string]bool{
	"true":  true,
	"1":     true,
	"false": false,
	"0":     false,
}

type httpServer struct {
	nsqd        *NSQD
	tlsEnabled  bool
	tlsRequired bool
	router      http.Handler
}

// newHTTPServer 初始化httpServer结构体对象
func newHTTPServer(nsqd *NSQD, tlsEnabled bool, tlsRequired bool) *httpServer {
	log := http_api.Log(nsqd.logf) // 初始化日志对象,通过此对象包装的方法会统计打印消耗时间信息

	router := httprouter.New()                                               // 初始化路由对象
	router.HandleMethodNotAllowed = true                                     // 如果启用，路由器会检查当前路由是否会被其他方法处理，如果当前请求无法被路由将以'Method Not Allowed'和HTTP状态码405来回答。如果没启用，该请求将被委托给NotFound处理程序。
	router.PanicHandler = http_api.LogPanicHandler(nsqd.logf)                // 初始化恐慌处理方法,处理从http处理程序恢复的恐慌的函数。它应该被用来生成一个错误页面并返回http错误代码500（内部服务器错误）。该处理程序可以用来防止你的服务器因为未恢复的恐慌而崩溃。
	router.NotFound = http_api.LogNotFoundHandler(nsqd.logf)                 // 可配置的http.Handler，当没有找到匹配的路由时被调用。如果它没有被设置，则使用http.NotFound。
	router.MethodNotAllowed = http_api.LogMethodNotAllowedHandler(nsqd.logf) // 可配置的http.Handler，当一个请求不能被路由且HandleMethodNotAllowed为真时，它被调用。如果没有设置，则使用http.Error和http.StatusMethodNotAllowed。在处理程序被调用之前，允许请求方法的 "Allow "头被设置。
	s := &httpServer{
		nsqd:        nsqd,        // 赋值nsqd对象
		tlsEnabled:  tlsEnabled,  // 赋值tls配置是否启用
		tlsRequired: tlsRequired, // 赋值tls证书是否需要验证
		router:      router,      // 赋值路由对象
	}

	router.Handle("GET", "/ping", http_api.Decorate(s.pingHandler, log, http_api.PlainText)) // 返回nsqd是否正常的心跳通知
	router.Handle("GET", "/info", http_api.Decorate(s.doInfo, log, http_api.V1))             // 输出客户端可重写的配置信息和nsqd对应的基础信息

	// v1 negotiate
	router.Handle("POST", "/pub", http_api.Decorate(s.doPUB, http_api.V1))         // 发布单条消息(可通过defer参数设置延迟消息)
	router.Handle("POST", "/mpub", http_api.Decorate(s.doMPUB, http_api.V1))       // 发布多条消息(仅发布实时消息)
	router.Handle("GET", "/stats", http_api.Decorate(s.doStats, log, http_api.V1)) // nsqd状态信息获取

	// only v1
	router.Handle("POST", "/topic/create", http_api.Decorate(s.doCreateTopic, log, http_api.V1))     // 根据topic名称创建topic对象
	router.Handle("POST", "/topic/delete", http_api.Decorate(s.doDeleteTopic, log, http_api.V1))     // 根据topic名称删除topic对象
	router.Handle("POST", "/topic/empty", http_api.Decorate(s.doEmptyTopic, log, http_api.V1))       // 根据topic名称情况topic对象中的所有消息对象
	router.Handle("POST", "/topic/pause", http_api.Decorate(s.doPauseTopic, log, http_api.V1))       // 根据topic名称暂停向所有channel分发消息
	router.Handle("POST", "/topic/unpause", http_api.Decorate(s.doPauseTopic, log, http_api.V1))     // 根据topic名称启动向所有channel分发消息
	router.Handle("POST", "/channel/create", http_api.Decorate(s.doCreateChannel, log, http_api.V1)) // topic存在的情况下创建channel对象
	router.Handle("POST", "/channel/delete", http_api.Decorate(s.doDeleteChannel, log, http_api.V1)) // 如果指定topic对象存在且channel对象也存在的情况下执行channel对象的删除逻辑
	router.Handle("POST", "/channel/empty", http_api.Decorate(s.doEmptyChannel, log, http_api.V1))   // 如果指定topic对象存在且channel对象也存在的情况下执行channel对象的清空逻辑
	router.Handle("POST", "/channel/pause", http_api.Decorate(s.doPauseChannel, log, http_api.V1))   // 如果指定topic对象存在且channel对象也存在的情况下暂停向所有客户端对象分发消息
	router.Handle("POST", "/channel/unpause", http_api.Decorate(s.doPauseChannel, log, http_api.V1)) // 如果指定topic对象存在且channel对象也存在的情况下启动向所有客户端对象分发消息
	router.Handle("GET", "/config/:opt", http_api.Decorate(s.doConfig, log, http_api.V1))            // :opt参数必须转化为下划线否则不会被识别到,获取对应的配置信息
	router.Handle("PUT", "/config/:opt", http_api.Decorate(s.doConfig, log, http_api.V1))            // :opt参数必须转化为下划线否则不会被识别到,更新配置(仅nsqlookupd_tcp_addresses和log_level)并获取更新后的此配置信息

	// debug
	router.HandlerFunc("GET", "/debug/pprof/", pprof.Index)
	router.HandlerFunc("GET", "/debug/pprof/cmdline", pprof.Cmdline)
	router.HandlerFunc("GET", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("POST", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("GET", "/debug/pprof/profile", pprof.Profile)
	router.Handler("GET", "/debug/pprof/heap", pprof.Handler("heap"))
	router.Handler("GET", "/debug/pprof/goroutine", pprof.Handler("goroutine"))
	router.Handler("GET", "/debug/pprof/block", pprof.Handler("block"))
	router.Handle("PUT", "/debug/setblockrate", http_api.Decorate(setBlockRateHandler, log, http_api.PlainText))
	router.Handle("POST", "/debug/freememory", http_api.Decorate(freeMemory, log, http_api.PlainText))
	router.Handler("GET", "/debug/pprof/threadcreate", pprof.Handler("threadcreate"))

	return s
}

func setBlockRateHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	rate, err := strconv.Atoi(req.FormValue("rate"))
	if err != nil {
		return nil, http_api.Err{Code: http.StatusBadRequest, Text: fmt.Sprintf("invalid block rate : %s", err.Error())}
	}
	runtime.SetBlockProfileRate(rate)
	return nil, nil
}

func freeMemory(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	debug.FreeOSMemory()
	return nil, nil
}

// ServeHTTP 实现了http.Handler接口的方法
func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if !s.tlsEnabled && s.tlsRequired { // 非https的情况下但需要验证客户端证书则返回证书未设置的错误提示信息
		resp := fmt.Sprintf(`{"message": "TLS_REQUIRED", "https_port": %d}`,
			s.nsqd.RealHTTPSAddr().Port)
		w.Header().Set("X-NSQ-Content-Type", "nsq; version=1.0")
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(403)
		io.WriteString(w, resp)
		return
	}
	s.router.ServeHTTP(w, req)
}

// pingHandler 返回nsqd是否正常的心跳通知
func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	health := s.nsqd.GetHealth()
	if !s.nsqd.IsHealthy() {
		return nil, http_api.Err{Code: 500, Text: health}
	}
	return health, nil
}

// doInfo 输出客户端可重写的配置信息和nsqd对应的基础信息
func (s *httpServer) doInfo(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return nil, http_api.Err{Code: 500, Text: err.Error()}
	}
	tcpPort := -1 // in case of unix socket
	if s.nsqd.RealTCPAddr().Network() == "tcp" {
		tcpPort = s.nsqd.RealTCPAddr().(*net.TCPAddr).Port
	}
	httpPort := -1 // in case of unix socket
	if s.nsqd.RealHTTPAddr().Network() == "tcp" {
		httpPort = s.nsqd.RealHTTPAddr().(*net.TCPAddr).Port
	}

	return struct {
		Version              string        `json:"version"`
		BroadcastAddress     string        `json:"broadcast_address"`
		Hostname             string        `json:"hostname"`
		HTTPPort             int           `json:"http_port"`
		TCPPort              int           `json:"tcp_port"`
		StartTime            int64         `json:"start_time"`
		MaxHeartBeatInterval time.Duration `json:"max_heartbeat_interval"`
		MaxOutBufferSize     int64         `json:"max_output_buffer_size"`
		MaxOutBufferTimeout  time.Duration `json:"max_output_buffer_timeout"`
		MaxDeflateLevel      int           `json:"max_deflate_level"`
	}{
		Version:              version.Binary,
		BroadcastAddress:     s.nsqd.getOpts().BroadcastAddress,
		Hostname:             hostname,
		TCPPort:              tcpPort,
		HTTPPort:             httpPort,
		StartTime:            s.nsqd.GetStartTime().Unix(),
		MaxHeartBeatInterval: s.nsqd.getOpts().MaxHeartbeatInterval,
		MaxOutBufferSize:     s.nsqd.getOpts().MaxOutputBufferSize,
		MaxOutBufferTimeout:  s.nsqd.getOpts().MaxOutputBufferTimeout,
		MaxDeflateLevel:      s.nsqd.getOpts().MaxDeflateLevel,
	}, nil
}

// getExistingTopicFromQuery 获取topic和channel名字并查询topic是否存在,存在的情况下返回topic对象和channel名称等信息
func (s *httpServer) getExistingTopicFromQuery(req *http.Request) (*http_api.ReqParams, *Topic, string, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		s.nsqd.logf(LOG_ERROR, "failed to parse request params - %s", err)
		return nil, nil, "", http_api.Err{Code: 400, Text: "INVALID_REQUEST"}
	}

	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		return nil, nil, "", http_api.Err{Code: 400, Text: err.Error()}
	}

	topic, err := s.nsqd.GetExistingTopic(topicName)
	if err != nil {
		return nil, nil, "", http_api.Err{Code: 404, Text: "TOPIC_NOT_FOUND"}
	}

	return reqParams, topic, channelName, err
}

// getTopicFromQuery 解析参数数据并获取或创建topic对象
func (s *httpServer) getTopicFromQuery(req *http.Request) (url.Values, *Topic, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		s.nsqd.logf(LOG_ERROR, "failed to parse request params - %s", err)
		return nil, nil, http_api.Err{Code: 400, Text: "INVALID_REQUEST"}
	}

	topicNames, ok := reqParams["topic"]
	if !ok {
		return nil, nil, http_api.Err{Code: 400, Text: "MISSING_ARG_TOPIC"}
	}
	topicName := topicNames[0]

	if !protocol.IsValidTopicName(topicName) {
		return nil, nil, http_api.Err{Code: 400, Text: "INVALID_TOPIC"}
	}

	return reqParams, s.nsqd.GetTopic(topicName), nil
}

// doPUB 发布单条消息(可通过defer参数设置延迟消息)
func (s *httpServer) doPUB(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// TODO: one day I'd really like to just error on chunked requests
	// to be able to fail "too big" requests before we even read

	if req.ContentLength > s.nsqd.getOpts().MaxMsgSize { // 当请求数据大于接收数据最大长度时返回413错误码(MSG_TOO_BIG)
		return nil, http_api.Err{Code: 413, Text: "MSG_TOO_BIG"}
	}

	// 可能存在实际消息体大小与传入参数ContentLength不符合的情况,所以设置长度多一位用于后面实际长度验证
	readMax := s.nsqd.getOpts().MaxMsgSize + 1
	body, err := io.ReadAll(io.LimitReader(req.Body, readMax))
	if err != nil { // 当读取数据异常时则返回500错误(INTERNAL_ERROR)
		return nil, http_api.Err{Code: 500, Text: "INTERNAL_ERROR"}
	}
	if int64(len(body)) == readMax { // 当实际读取到的数据长度大于接收最大长度时返回413错误码(MSG_TOO_BIG)
		return nil, http_api.Err{Code: 413, Text: "MSG_TOO_BIG"}
	}
	if len(body) == 0 { // 当请求数据为空时返回400错误码(MSG_EMPTY)
		return nil, http_api.Err{Code: 400, Text: "MSG_EMPTY"}
	}

	reqParams, topic, err := s.getTopicFromQuery(req) // 解析参数数据并获取或创建topic对象
	if err != nil {                                   // 参数获取失败则返回400错误码(INVALID_REQUEST)
		return nil, err
	}

	// 检查是否发布延迟消息
	// 1.设置的情况下
	//	1).如果延迟时间解析异常或在nsqd服务设置范围外则返回400错误码(INVALID_DEFER)
	//	2).延迟时间在设置范围内则继续下面的流程
	var deferred time.Duration
	if ds, ok := reqParams["defer"]; ok {
		var di int64
		di, err = strconv.ParseInt(ds[0], 10, 64)
		if err != nil {
			return nil, http_api.Err{Code: 400, Text: "INVALID_DEFER"}
		}
		deferred = time.Duration(di) * time.Millisecond
		if deferred < 0 || deferred > s.nsqd.getOpts().MaxReqTimeout {
			return nil, http_api.Err{Code: 400, Text: "INVALID_DEFER"}
		}
	}

	msg := NewMessage(topic.GenerateID(), body) // 根据请求的消息体和topic对象生成的id初始化消息对象
	msg.deferred = deferred
	err = topic.PutMessage(msg) // 向topic的队列中写入一个消息对象
	if err != nil {             // 客户端连接处于退出状态或写入失败则返回503状态码(EXITING)
		return nil, http_api.Err{Code: 503, Text: "EXITING"}
	}

	return "OK", nil
}

// doMPUB 发布多条消息(仅发布实时消息)
func (s *httpServer) doMPUB(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	var msgs []*Message
	var exit bool

	// TODO: one day I'd really like to just error on chunked requests
	// to be able to fail "too big" requests before we even read

	if req.ContentLength > s.nsqd.getOpts().MaxBodySize { // 当请求数据大于接收数据最大长度时返回413错误码(MSG_TOO_BIG)
		return nil, http_api.Err{Code: 413, Text: "BODY_TOO_BIG"}
	}

	reqParams, topic, err := s.getTopicFromQuery(req) // 解析参数数据并获取或创建topic对象
	if err != nil {
		return nil, err
	}

	// 默认文本模式,若设置了binary参数则通过字符串转化成bool变量判断是否使用二进制模式,若不在boolParams中则默认启用二进制模式
	binaryMode := false
	if vals, ok := reqParams["binary"]; ok {
		if binaryMode, ok = boolParams[vals[0]]; !ok {
			binaryMode = true
			s.nsqd.logf(LOG_WARN, "deprecated value '%s' used for /mpub binary param", vals[0])
		}
	}
	if binaryMode { // 二进制模式下发布多条任务
		tmp := make([]byte, 4)
		msgs, err = readMPUB(req.Body, tmp, topic,
			s.nsqd.getOpts().MaxMsgSize, s.nsqd.getOpts().MaxBodySize)
		if err != nil {
			return nil, http_api.Err{Code: 413, Text: err.(*protocol.FatalClientErr).Code[2:]}
		}
	} else { // 文本模式下发布多条任务
		// add 1 so that it's greater than our max when we test for it
		// (LimitReader returns a "fake" EOF)
		// 可能存在实际消息体大小与传入参数ContentLength不符合的情况,所以设置长度多一位用于后面实际长度验证
		readMax := s.nsqd.getOpts().MaxBodySize + 1
		rdr := bufio.NewReader(io.LimitReader(req.Body, readMax))
		total := 0
		for !exit {
			var block []byte
			block, err = rdr.ReadBytes('\n') // 文本方式读取到第一个换行符\n时结束并返回块信息和错误信息
			if err != nil {                  // 存在错误则判断是否读到最后一条消息,结束符(io.EOF)
				if err != io.EOF { // 非结束符io.EOF则返回状态码500(INTERNAL_ERROR)
					return nil, http_api.Err{Code: 500, Text: "INTERNAL_ERROR"}
				}
				exit = true
			}
			total += len(block)          // 读取数据总长度累加
			if int64(total) == readMax { // 当实际读取到的数据长度大于接收最大长度时返回413错误码(BODY_TOO_BIG)
				return nil, http_api.Err{Code: 413, Text: "BODY_TOO_BIG"}
			}

			if len(block) > 0 && block[len(block)-1] == '\n' {
				block = block[:len(block)-1] // 切割字符串末尾的换行符\n
			}

			// 默默地丢弃0长度的消息，这保持了0.2.22之前的行为。
			if len(block) == 0 {
				continue
			}

			if int64(len(block)) > s.nsqd.getOpts().MaxMsgSize {
				return nil, http_api.Err{Code: 413, Text: "MSG_TOO_BIG"}
			}

			msg := NewMessage(topic.GenerateID(), block)
			msgs = append(msgs, msg)
		}
	}

	err = topic.PutMessages(msgs)
	if err != nil {
		return nil, http_api.Err{Code: 503, Text: "EXITING"}
	}

	return "OK", nil
}

// doCreateTopic 根据topic名称创建topic对象
func (s *httpServer) doCreateTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, _, err := s.getTopicFromQuery(req)
	return nil, err
}

// doEmptyTopic 根据topic名称情况topic对象中的所有消息对象
func (s *httpServer) doEmptyTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		s.nsqd.logf(LOG_ERROR, "failed to parse request params - %s", err)
		return nil, http_api.Err{Code: 400, Text: "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{Code: 400, Text: "MISSING_ARG_TOPIC"}
	}

	if !protocol.IsValidTopicName(topicName) {
		return nil, http_api.Err{Code: 400, Text: "INVALID_TOPIC"}
	}

	topic, err := s.nsqd.GetExistingTopic(topicName)
	if err != nil {
		return nil, http_api.Err{Code: 404, Text: "TOPIC_NOT_FOUND"}
	}

	err = topic.Empty()
	if err != nil {
		return nil, http_api.Err{Code: 500, Text: "INTERNAL_ERROR"}
	}

	return nil, nil
}

// doDeleteTopic 根据topic名称删除topic对象
func (s *httpServer) doDeleteTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		s.nsqd.logf(LOG_ERROR, "failed to parse request params - %s", err)
		return nil, http_api.Err{Code: 400, Text: "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{Code: 400, Text: "MISSING_ARG_TOPIC"}
	}

	err = s.nsqd.DeleteExistingTopic(topicName)
	if err != nil {
		return nil, http_api.Err{Code: 404, Text: "TOPIC_NOT_FOUND"}
	}

	return nil, nil
}

// doPauseTopic 根据topic名称暂停或启动向所有channel分发消息
func (s *httpServer) doPauseTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		s.nsqd.logf(LOG_ERROR, "failed to parse request params - %s", err)
		return nil, http_api.Err{Code: 400, Text: "INVALID_REQUEST"}
	}

	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{Code: 400, Text: "MISSING_ARG_TOPIC"}
	}

	topic, err := s.nsqd.GetExistingTopic(topicName)
	if err != nil {
		return nil, http_api.Err{Code: 404, Text: "TOPIC_NOT_FOUND"}
	}

	if strings.Contains(req.URL.Path, "unpause") {
		err = topic.UnPause()
	} else {
		err = topic.Pause()
	}
	if err != nil {
		s.nsqd.logf(LOG_ERROR, "failure in %s - %s", req.URL.Path, err)
		return nil, http_api.Err{Code: 500, Text: "INTERNAL_ERROR"}
	}

	// 主动持久化元数据，以便在进程失败的情况下，nsqd不会突然（取消）暂停主题topic
	s.nsqd.Lock()
	s.nsqd.PersistMetadata()
	s.nsqd.Unlock()
	return nil, nil
}

// doCreateChannel topic存在的情况下创建channel对象
func (s *httpServer) doCreateChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicFromQuery(req)
	if err != nil {
		return nil, err
	}
	topic.GetChannel(channelName)
	return nil, nil
}

// doEmptyChannel 如果指定topic对象存在且channel对象也存在的情况下执行channel对象的清空逻辑
func (s *httpServer) doEmptyChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicFromQuery(req)
	if err != nil {
		return nil, err
	}

	channel, err := topic.GetExistingChannel(channelName)
	if err != nil {
		return nil, http_api.Err{Code: 404, Text: "CHANNEL_NOT_FOUND"}
	}

	err = channel.Empty()
	if err != nil {
		return nil, http_api.Err{Code: 500, Text: "INTERNAL_ERROR"}
	}

	return nil, nil
}

// doDeleteChannel 如果指定topic对象存在且channel对象也存在的情况下执行channel对象的删除逻辑
func (s *httpServer) doDeleteChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicFromQuery(req)
	if err != nil {
		return nil, err
	}

	err = topic.DeleteExistingChannel(channelName)
	if err != nil {
		return nil, http_api.Err{Code: 404, Text: "CHANNEL_NOT_FOUND"}
	}

	return nil, nil
}

// doPauseChannel 如果指定topic对象存在且channel对象也存在的情况下暂停或启动向所有客户端对象分发消息
func (s *httpServer) doPauseChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	_, topic, channelName, err := s.getExistingTopicFromQuery(req)
	if err != nil {
		return nil, err
	}

	channel, err := topic.GetExistingChannel(channelName)
	if err != nil {
		return nil, http_api.Err{Code: 404, Text: "CHANNEL_NOT_FOUND"}
	}

	if strings.Contains(req.URL.Path, "unpause") {
		err = channel.UnPause()
	} else {
		err = channel.Pause()
	}
	if err != nil {
		s.nsqd.logf(LOG_ERROR, "failure in %s - %s", req.URL.Path, err)
		return nil, http_api.Err{Code: 500, Text: "INTERNAL_ERROR"}
	}

	// 主动持久化元数据，以便在进程失败的情况下，nsqd不会突然（取消）暂停频道channel
	s.nsqd.Lock()
	s.nsqd.PersistMetadata()
	s.nsqd.Unlock()
	return nil, nil
}

// doStats nsqd状态信息获取,可设置参数包含
//
//	1.format: 格式化方式(text|json)
//	2.topic: 获取的topic
//	3.channel: 获取的channel
//	4.include_clients: 是否需要获取对应channel的客户端信息
//	5.include_mem: 是否需要获取nsqd对象的内存和堆栈等信息
func (s *httpServer) doStats(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		s.nsqd.logf(LOG_ERROR, "failed to parse request params - %s", err)
		return nil, http_api.Err{Code: 400, Text: "INVALID_REQUEST"}
	}
	formatString, _ := reqParams.Get("format")
	topicName, _ := reqParams.Get("topic")
	channelName, _ := reqParams.Get("channel")
	includeClientsParam, _ := reqParams.Get("include_clients")
	includeMemParam, _ := reqParams.Get("include_mem")
	jsonFormat := formatString == "json"

	includeClients, ok := boolParams[includeClientsParam]
	if !ok {
		includeClients = true
	}
	includeMem, ok := boolParams[includeMemParam]
	if !ok {
		includeMem = true
	}

	stats := s.nsqd.GetStats(topicName, channelName, includeClients)
	health := s.nsqd.GetHealth()
	startTime := s.nsqd.GetStartTime()
	uptime := time.Since(startTime)

	var ms *memStats
	if includeMem {
		m := getMemStats()
		ms = &m
	}
	if !jsonFormat {
		return s.printStats(stats, ms, health, startTime, uptime), nil
	}

	// TODO: should producer stats be hung off topics?
	return struct {
		Version   string        `json:"version"`
		Health    string        `json:"health"`
		StartTime int64         `json:"start_time"`
		Topics    []TopicStats  `json:"topics"`
		Memory    *memStats     `json:"memory,omitempty"`
		Producers []ClientStats `json:"producers"`
	}{version.Binary, health, startTime.Unix(), stats.Topics, ms, stats.Producers}, nil
}

func (s *httpServer) printStats(stats Stats, ms *memStats, health string, startTime time.Time, uptime time.Duration) []byte {
	var buf bytes.Buffer
	w := &buf

	fmt.Fprintf(w, "%s\n", version.String("nsqd"))
	fmt.Fprintf(w, "start_time %v\n", startTime.Format(time.RFC3339))
	fmt.Fprintf(w, "uptime %s\n", uptime)

	fmt.Fprintf(w, "\nHealth: %s\n", health)

	if ms != nil {
		fmt.Fprintf(w, "\nMemory:\n")
		fmt.Fprintf(w, "   %-25s\t%d\n", "heap_objects", ms.HeapObjects)
		fmt.Fprintf(w, "   %-25s\t%d\n", "heap_idle_bytes", ms.HeapIdleBytes)
		fmt.Fprintf(w, "   %-25s\t%d\n", "heap_in_use_bytes", ms.HeapInUseBytes)
		fmt.Fprintf(w, "   %-25s\t%d\n", "heap_released_bytes", ms.HeapReleasedBytes)
		fmt.Fprintf(w, "   %-25s\t%d\n", "gc_pause_usec_100", ms.GCPauseUsec100)
		fmt.Fprintf(w, "   %-25s\t%d\n", "gc_pause_usec_99", ms.GCPauseUsec99)
		fmt.Fprintf(w, "   %-25s\t%d\n", "gc_pause_usec_95", ms.GCPauseUsec95)
		fmt.Fprintf(w, "   %-25s\t%d\n", "next_gc_bytes", ms.NextGCBytes)
		fmt.Fprintf(w, "   %-25s\t%d\n", "gc_total_runs", ms.GCTotalRuns)
	}

	if len(stats.Topics) == 0 {
		fmt.Fprintf(w, "\nTopics: None\n")
	} else {
		fmt.Fprintf(w, "\nTopics:")
	}

	for _, t := range stats.Topics {
		var pausedPrefix string
		if t.Paused {
			pausedPrefix = "*P "
		} else {
			pausedPrefix = "   "
		}
		fmt.Fprintf(w, "\n%s[%-15s] depth: %-5d be-depth: %-5d msgs: %-8d e2e%%: %s\n",
			pausedPrefix,
			t.TopicName,
			t.Depth,
			t.BackendDepth,
			t.MessageCount,
			t.E2eProcessingLatency,
		)
		for _, c := range t.Channels {
			if c.Paused {
				pausedPrefix = "   *P "
			} else {
				pausedPrefix = "      "
			}
			fmt.Fprintf(w, "%s[%-25s] depth: %-5d be-depth: %-5d inflt: %-4d def: %-4d re-q: %-5d timeout: %-5d msgs: %-8d e2e%%: %s\n",
				pausedPrefix,
				c.ChannelName,
				c.Depth,
				c.BackendDepth,
				c.InFlightCount,
				c.DeferredCount,
				c.RequeueCount,
				c.TimeoutCount,
				c.MessageCount,
				c.E2eProcessingLatency,
			)
			for _, client := range c.Clients {
				fmt.Fprintf(w, "        %s\n", client)
			}
		}
	}

	if len(stats.Producers) == 0 {
		fmt.Fprintf(w, "\nProducers: None\n")
	} else {
		fmt.Fprintf(w, "\nProducers:\n")
		for _, client := range stats.Producers {
			fmt.Fprintf(w, "   %s\n", client)
		}
	}

	return buf.Bytes()
}

// doConfig 更新配置(仅nsqlookupd_tcp_addresses和log_level)或获取对应的配置信息
func (s *httpServer) doConfig(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	opt := ps.ByName("opt")

	if req.Method == "PUT" {
		// add 1 so that it's greater than our max when we test for it
		// (LimitReader returns a "fake" EOF)
		readMax := s.nsqd.getOpts().MaxMsgSize + 1
		body, err := io.ReadAll(io.LimitReader(req.Body, readMax))
		if err != nil {
			return nil, http_api.Err{Code: 500, Text: "INTERNAL_ERROR"}
		}
		if int64(len(body)) == readMax || len(body) == 0 {
			return nil, http_api.Err{Code: 413, Text: "INVALID_VALUE"}
		}

		opts := *s.nsqd.getOpts()
		switch opt {
		case "nsqlookupd_tcp_addresses":
			err := json.Unmarshal(body, &opts.NSQLookupdTCPAddresses)
			if err != nil {
				return nil, http_api.Err{Code: 400, Text: "INVALID_VALUE"}
			}
		case "log_level":
			logLevelStr := string(body)
			logLevel, err := lg.ParseLogLevel(logLevelStr)
			if err != nil {
				return nil, http_api.Err{Code: 400, Text: "INVALID_VALUE"}
			}
			opts.LogLevel = logLevel
		default:
			return nil, http_api.Err{Code: 400, Text: "INVALID_OPTION"}
		}
		s.nsqd.swapOpts(&opts)
		s.nsqd.triggerOptsNotification()
	}

	v, ok := getOptByCfgName(s.nsqd.getOpts(), opt)
	if !ok {
		return nil, http_api.Err{Code: 400, Text: "INVALID_OPTION"}
	}

	return v, nil
}

func getOptByCfgName(opts interface{}, name string) (interface{}, bool) {
	val := reflect.ValueOf(opts).Elem()
	typ := val.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		flagName := field.Tag.Get("flag")
		cfgName := field.Tag.Get("cfg")
		if flagName == "" {
			continue
		}
		if cfgName == "" {
			cfgName = strings.Replace(flagName, "-", "_", -1)
		}
		if name != cfgName {
			continue
		}
		return val.FieldByName(field.Name).Interface(), true
	}
	return nil, false
}
