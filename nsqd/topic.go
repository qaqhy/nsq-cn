package nsqd

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-diskqueue"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/quantile"
	"github.com/nsqio/nsq/internal/util"
)

type Topic struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messageCount uint64
	messageBytes uint64

	sync.RWMutex

	name              string
	channelMap        map[string]*Channel
	backend           BackendQueue
	memoryMsgChan     chan *Message
	startChan         chan int
	exitChan          chan int
	channelUpdateChan chan int
	waitGroup         util.WaitGroupWrapper
	exitFlag          int32
	idFactory         *guidFactory

	ephemeral      bool
	deleteCallback func(*Topic)
	deleter        sync.Once

	paused    int32
	pauseChan chan int

	nsqd *NSQD
}

// NewTopic 生成一个topic对象
func NewTopic(topicName string, nsqd *NSQD, deleteCallback func(*Topic)) *Topic {
	t := &Topic{
		name:              topicName,
		channelMap:        make(map[string]*Channel),                        // 初始化频道channelMap对象
		memoryMsgChan:     make(chan *Message, nsqd.getOpts().MemQueueSize), // 初始化指定大小的内存消息通道，对应参数mem-queue-size。默认10000
		startChan:         make(chan int, 1),                                // 初始化启动通道，由方法t.Start()触发通知
		exitChan:          make(chan int),                                   // 初始化退出通道，由(t *Topic) Delete()和(t *Topic) Close()两个方法触发通知
		channelUpdateChan: make(chan int),                                   // 初始化频道channel更新通道，当此topic下频道有新增或删除时触发通知
		nsqd:              nsqd,                                             // 初始化nsqd对象，用于便捷获取对象中的参数和此topic下的消息推送失败等信息同步以及对象中Notify方法的调用
		paused:            0,                                                // 初始化暂停信号【0:启动，1:暂停】，用于此topic对象是否向下面所有频道channel分发消息
		pauseChan:         make(chan int),                                   // 初始化暂停通道，当触发暂停或启动时会通知消息泵messagePump更新是否继续分发消息状态
		deleteCallback:    deleteCallback,                                   // 初始化删除回调方法，此topic若时临时的ephemeral=true，在没有频道channel对象的情况下将调用此方法
		idFactory:         NewGUIDFactory(nsqd.getOpts().ID),                // 根据nsqd对象的node-id生成一个guid工厂，用于生成消息对象唯一的消息id
	}
	if strings.HasSuffix(topicName, "#ephemeral") { // topic名字包含#ephemeral后缀则视为临时topic对象
		t.ephemeral = true                 // 标记为临时topic
		t.backend = newDummyBackendQueue() // 初始化一个虚拟磁盘队列，仅临时topic对象占位使用，无任何作用
	} else {
		dqLogf := func(level diskqueue.LogLevel, f string, args ...interface{}) {
			opts := nsqd.getOpts()
			lg.Logf(opts.Logger, opts.LogLevel, lg.LogLevel(level), f, args...)
		}
		t.backend = diskqueue.New( // ***重点：初始化磁盘队列对象，此方法中存在磁盘队列的管理逻辑，下面会提出来详解
			topicName,
			nsqd.getOpts().DataPath,        // 磁盘数据存放根路径data-path，没有默认值，若为空则设置当前可执行文件的上级路径为磁盘数据存放根路径
			nsqd.getOpts().MaxBytesPerFile, // 每个磁盘文件存储的最大容量max-bytes-per-file，默认100MB
			int32(minValidMsgLength),       // 最小有效消息长度26(16+8+2)
			int32(nsqd.getOpts().MaxMsgSize)+minValidMsgLength, // 最大有效消息长度1048602(1024*1024+16+8+2)
			nsqd.getOpts().SyncEvery,                           // 设置磁盘队列的读取操作次数值sync-every，达到此值时异步持久化一次fsync（默认2500次）
			nsqd.getOpts().SyncTimeout,                         // 设置磁盘队列的轮循时间sync-timeout，达到此值时异步持久化一次fsync（默认为2s）
			dqLogf,                                             // 磁盘队列日志对象
		)
	}

	t.waitGroup.Wrap(t.messagePump) // ***重点：异步调用messagePump方法，此方法逻辑相对比较复杂，下面会提出来详解

	t.nsqd.Notify(t, !t.ephemeral) // 异步调用nsqd.Notify方法将topic对象通过notifyChan通道注册到所有的lookup服务中

	return t
}

// Start 通过通道通知此topic下的消息泵启动
func (t *Topic) Start() {
	select {
	case t.startChan <- 1:
	default:
	}
}

// Exiting returns a boolean indicating if this topic is closed/exiting
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
func (t *Topic) GetChannel(channelName string) *Channel {
	t.Lock()
	channel, isNew := t.getOrCreateChannel(channelName)
	t.Unlock()

	if isNew {
		// update messagePump state
		select {
		case t.channelUpdateChan <- 1:
		case <-t.exitChan:
		}
	}

	return channel
}

// this expects the caller to handle locking
func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	channel, ok := t.channelMap[channelName]
	if !ok {
		deleteCallback := func(c *Channel) {
			t.DeleteExistingChannel(c.name)
		}
		channel = NewChannel(t.name, channelName, t.nsqd, deleteCallback)
		t.channelMap[channelName] = channel
		t.nsqd.logf(LOG_INFO, "TOPIC(%s): new channel(%s)", t.name, channel.name)
		return channel, true
	}
	return channel, false
}

func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.RLock()
	defer t.RUnlock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		return nil, errors.New("channel does not exist")
	}
	return channel, nil
}

// DeleteExistingChannel removes a channel from the topic only if it exists
func (t *Topic) DeleteExistingChannel(channelName string) error {
	t.RLock()
	channel, ok := t.channelMap[channelName]
	t.RUnlock()
	if !ok {
		return errors.New("channel does not exist")
	}

	t.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting channel %s", t.name, channel.name)

	// delete empties the channel before closing
	// (so that we dont leave any messages around)
	//
	// we do this before removing the channel from map below (with no lock)
	// so that any incoming subs will error and not create a new channel
	// to enforce ordering
	channel.Delete()

	t.Lock()
	delete(t.channelMap, channelName)
	numChannels := len(t.channelMap)
	t.Unlock()

	// update messagePump state
	select {
	case t.channelUpdateChan <- 1:
	case <-t.exitChan:
	}

	if numChannels == 0 && t.ephemeral {
		go t.deleter.Do(func() { t.deleteCallback(t) })
	}

	return nil
}

// PutMessage writes a Message to the queue
func (t *Topic) PutMessage(m *Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	err := t.put(m)
	if err != nil {
		return err
	}
	atomic.AddUint64(&t.messageCount, 1)
	atomic.AddUint64(&t.messageBytes, uint64(len(m.Body)))
	return nil
}

// PutMessages writes multiple Messages to the queue
func (t *Topic) PutMessages(msgs []*Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}

	messageTotalBytes := 0

	for i, m := range msgs {
		err := t.put(m)
		if err != nil {
			atomic.AddUint64(&t.messageCount, uint64(i))
			atomic.AddUint64(&t.messageBytes, uint64(messageTotalBytes))
			return err
		}
		messageTotalBytes += len(m.Body)
	}

	atomic.AddUint64(&t.messageBytes, uint64(messageTotalBytes))
	atomic.AddUint64(&t.messageCount, uint64(len(msgs)))
	return nil
}

// put 将消息对象推送到topic下的队列中(内存管道或磁盘队列)
func (t *Topic) put(m *Message) error {
	// 如果mem-queue-size == 0则跳过下面逻辑，避免使用内存通道，以获得更一致的排序,
	// 但尽量使用内存通道来处理延迟消息（因为它们在后端磁盘队列中会失去延迟时间）或
	// 如果topic是短暂的（没有后端队列）将执行下面的逻辑。
	if cap(t.memoryMsgChan) > 0 || t.ephemeral || m.deferred != 0 {
		select {
		case t.memoryMsgChan <- m:
			return nil
		default:
			break // 写入到磁盘队列中
		}
	}
	err := writeMessageToBackend(m, t.backend)
	t.nsqd.SetHealth(err)
	if err != nil {
		t.nsqd.logf(LOG_ERROR,
			"TOPIC(%s) ERROR: failed to write message to backend - %s",
			t.name, err)
		return err
	}
	return nil
}

func (t *Topic) Depth() int64 {
	return int64(len(t.memoryMsgChan)) + t.backend.Depth()
}

// messagePump 在内存和后端队列中进行选择，并为这个主题的每个通道写入消息。
// 此方法是nsqd的核心方法之一（消息核心管理）
func (t *Topic) messagePump() {
	var msg *Message
	var buf []byte
	var err error
	var chans []*Channel
	var memoryMsgChan chan *Message
	var backendChan <-chan []byte

	// 不要在Start()之前传递消息，但要避免Pause()或GetChannel()方法引起对应通道阻塞
	for {
		select {
		case <-t.channelUpdateChan: // 此topic下的频道channel有新增或删除时触发通知，这里避免阻塞
			continue
		case <-t.pauseChan: // 此topic有暂停或启动操作时触发通知，这里避免阻塞
			continue
		case <-t.exitChan: // 此topic收到退出信号时，直接走退出流程
			goto exit
		case <-t.startChan: // 收到topic的启动通知，启动消息泵开始处理数据
		}
		break
	}
	t.RLock()
	for _, c := range t.channelMap { // 读锁下加载此topic下的所有频道channel对象
		chans = append(chans, c)
	}
	t.RUnlock()
	if len(chans) > 0 && !t.IsPaused() { // 此topic下存在频道且topic是启动状态时更新监听的内存消息通道对象和磁盘数据通道对象
		memoryMsgChan = t.memoryMsgChan
		backendChan = t.backend.ReadChan()
	}

	// 消息循环（核心）
	for {
		select {
		case msg = <-memoryMsgChan: // 实时接受内存消息通道提供的消息对象
		case buf = <-backendChan: // 实时接受磁盘数据通道提供的消息数据
			msg, err = decodeMessage(buf) // 解码消息数据到消息对象中
			if err != nil {
				t.nsqd.logf(LOG_ERROR, "failed to decode message - %s", err)
				continue
			}
		case <-t.channelUpdateChan: // 此topic下的频道channel有新增或删除通知，这里收到后及时更新频道channel列表
			chans = chans[:0] // 删除频道列表中的所有频道
			t.RLock()
			for _, c := range t.channelMap { // 读锁下加载此topic下的所有频道channel对象
				chans = append(chans, c)
			}
			t.RUnlock()
			if len(chans) == 0 || t.IsPaused() { // 此topic下不存在频道或topic是暂停状态时关闭监听的内存消息通道对象和磁盘数据通道对象
				memoryMsgChan = nil
				backendChan = nil
			} else { // 此topic下存在频道且topic是启动状态时更新监听的内存消息通道对象和磁盘数据通道对象
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case <-t.pauseChan: // 收到启动或暂停通知时，检查是否需要关闭或更新监听的内存消息通道对象和磁盘数据通道对象
			if len(chans) == 0 || t.IsPaused() {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case <-t.exitChan: // 此topic收到退出信号时，直接走退出流程
			goto exit
		}

		for i, channel := range chans {
			chanMsg := msg
			// 复制消息对象，因为每个通道都需要一个唯一的实例
			// 如果是第一个频道channel（主题已经创建了第一个消息对象），则跳过复制
			if i > 0 {
				chanMsg = NewMessage(msg.ID, msg.Body) // 保证每个频道获取的消息对象对应的地址唯一
				chanMsg.Timestamp = msg.Timestamp
				chanMsg.deferred = msg.deferred
			}
			if chanMsg.deferred != 0 { // 如果时延迟消息，则放到延迟队列中
				channel.PutMessageDeferred(chanMsg, chanMsg.deferred) // 放到延迟队列前会根据延迟时间升序排序，调用container/heap包下的heap.Push方法
				continue
			}
			err := channel.PutMessage(chanMsg) // 将消息放到频道channel的实时消息队列中（内存消息通道或磁盘消息队列）
			if err != nil {
				t.nsqd.logf(LOG_ERROR,
					"TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s",
					t.name, msg.ID, channel.name, err)
			}
		}
	}

exit:
	t.nsqd.logf(LOG_INFO, "TOPIC(%s): closing ... messagePump", t.name)
}

// Delete 清空主题及其所有频道
func (t *Topic) Delete() error {
	return t.exit(true)
}

// Close 关闭主题及其所有频道
func (t *Topic) Close() error {
	return t.exit(false)
}

// exit topic对象的删除或者关闭(由参数控制)
func (t *Topic) exit(deleted bool) error {
	if !atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1) { // 如果默认值是1则说明此方法被重复调用将直接退出,否则继续执行下面的删除或关闭逻辑
		return errors.New("exiting")
	}

	if deleted { // 删除的情况将调用通知方法通知lookupLoop协程中注销此topic及所有频道channel
		t.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting", t.name)

		// since we are explicitly deleting a topic (not just at system exit time)
		// de-register this from the lookupd
		t.nsqd.Notify(t, !t.ephemeral)
	} else {
		t.nsqd.logf(LOG_INFO, "TOPIC(%s): closing", t.name)
	}

	close(t.exitChan)

	// synchronize the close of messagePump()
	t.waitGroup.Wait()

	if deleted {
		t.Lock()
		for _, channel := range t.channelMap {
			delete(t.channelMap, channel.name)
			channel.Delete()
		}
		t.Unlock()

		// empty the queue (deletes the backend files, too)
		t.Empty()
		return t.backend.Delete()
	}

	// close all the channels
	t.RLock()
	for _, channel := range t.channelMap {
		err := channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			t.nsqd.logf(LOG_ERROR, "channel(%s) close - %s", channel.name, err)
		}
	}
	t.RUnlock()

	// write anything leftover to disk
	t.flush()
	return t.backend.Close()
}

func (t *Topic) Empty() error {
	for {
		select {
		case <-t.memoryMsgChan:
		default:
			goto finish
		}
	}

finish:
	return t.backend.Empty()
}

func (t *Topic) flush() error {
	if len(t.memoryMsgChan) > 0 {
		t.nsqd.logf(LOG_INFO,
			"TOPIC(%s): flushing %d memory messages to backend",
			t.name, len(t.memoryMsgChan))
	}

	for {
		select {
		case msg := <-t.memoryMsgChan:
			err := writeMessageToBackend(msg, t.backend)
			if err != nil {
				t.nsqd.logf(LOG_ERROR,
					"ERROR: failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}

finish:
	return nil
}

func (t *Topic) AggregateChannelE2eProcessingLatency() *quantile.Quantile {
	var latencyStream *quantile.Quantile
	t.RLock()
	realChannels := make([]*Channel, 0, len(t.channelMap))
	for _, c := range t.channelMap {
		realChannels = append(realChannels, c)
	}
	t.RUnlock()
	for _, c := range realChannels {
		if c.e2eProcessingLatencyStream == nil {
			continue
		}
		if latencyStream == nil {
			latencyStream = quantile.New(
				t.nsqd.getOpts().E2EProcessingLatencyWindowTime,
				t.nsqd.getOpts().E2EProcessingLatencyPercentiles)
		}
		latencyStream.Merge(c.e2eProcessingLatencyStream)
	}
	return latencyStream
}

func (t *Topic) Pause() error {
	return t.doPause(true)
}

func (t *Topic) UnPause() error {
	return t.doPause(false)
}

func (t *Topic) doPause(pause bool) error {
	if pause {
		atomic.StoreInt32(&t.paused, 1)
	} else {
		atomic.StoreInt32(&t.paused, 0)
	}

	select {
	case t.pauseChan <- 1:
	case <-t.exitChan:
	}

	return nil
}

func (t *Topic) IsPaused() bool {
	return atomic.LoadInt32(&t.paused) == 1
}

func (t *Topic) GenerateID() MessageID {
	var i int64 = 0
	for {
		id, err := t.idFactory.NewGUID()
		if err == nil {
			return id.Hex()
		}
		if i%10000 == 0 {
			t.nsqd.logf(LOG_ERROR, "TOPIC(%s): failed to create guid - %s", t.name, err)
		}
		time.Sleep(time.Millisecond)
		i++
	}
}
