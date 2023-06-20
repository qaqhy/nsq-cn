package nsqd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

const (
	MsgIDLength       = 16                  // 消息ID的长度
	minValidMsgLength = MsgIDLength + 8 + 2 // Timestamp + Attempts 最小有效的消息内容长度
)

var deferMsgMagicFlag = []byte("#DEFER_MSG#")

type MessageID [MsgIDLength]byte

type Message struct {
	ID        MessageID // ID (持久化)
	Body      []byte    // 消息体 (持久化)
	Timestamp int64     // 产生的时间戳 (持久化)
	Attempts  uint16    // 消息尝试发送的次数 (持久化)

	// for in-flight handling
	deliveryTS time.Time // 消息被发送时的时间戳
	clientID   int64     // 产生消息的客户端 ID
	// NSQ 默认按照消息的时间戳来处理消息, 如果需要指定优先级, 那么就需要指定 pri 字段, NSQ 会根据优先级写入 inFlightQueue 中.
	// 默认 pri 是规定的消息最晚需要被消费的时间
	pri      int64         // 消息最晚需要被消费的时间
	index    int           // 在 InFlight Queue中的下标
	deferred time.Duration // 消息的延迟发送时间
}

// NewMessage 生成消息对象
func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		ID:        id,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

// WriteTo 将消息数据写入到w流中
func (m *Message) WriteTo(w io.Writer) (int64, error) {
	var buf [10]byte
	var total int64

	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], uint16(m.Attempts))

	n, err := w.Write(buf[:]) // 前8字节写入时间戳信息,8-10字节写入重试次数信息
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.ID[:]) // 10-26字节写入消息ID信息
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.Body) // 26字节后写入消息体信息
	total += int64(n)
	if err != nil {
		return total, err
	}

	if m.deferred > 0 {
		n, err = w.Write(deferMsgMagicFlag)
		total += int64(n)
		if err != nil {
			return total, err
		}

		var deferBuf [8]byte
		var expire = time.Now().Add(m.deferred).UnixNano()
		binary.BigEndian.PutUint64(deferBuf[:8], uint64(expire))

		n, err := w.Write(deferBuf[:])
		total += int64(n)
		if err != nil {
			return total, err
		}
	}

	return total, nil
}

// decodeMessage deserializes data (as []byte) and creates a new Message
//
//	[x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...[x][x][x][x][x][x][x][x]
//	|       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)    ||       (int64)
//	|       8-byte         ||    ||                 16-byte                      || N-byte      ||       8-byte
//	------------------------------------------------------------------------------------------------------------------...
//	  nanosecond timestamp    ^^                   message ID                       message body    nanosecond expire
//	                       (uint16)
//	                        2-byte
//	                       attempts
func decodeMessage(b []byte) (*Message, error) {
	var msg Message

	if len(b) < minValidMsgLength {
		return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
	}

	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])
	copy(msg.ID[:], b[10:10+MsgIDLength])

	if bytes.Equal(b[len(b)-8-len(deferMsgMagicFlag):len(b)-8], deferMsgMagicFlag) {
		expire := int64(binary.BigEndian.Uint64(b[len(b)-8:]))
		ts := time.Now().UnixNano()
		if expire > ts {
			msg.deferred = time.Duration(expire - ts)
		}
		msg.Body = b[10+MsgIDLength : len(b)-8-len(deferMsgMagicFlag)]
	} else {
		msg.Body = b[10+MsgIDLength:]
	}

	return &msg, nil
}

// writeMessageToBackend 将消息对象写到磁盘队列中
func writeMessageToBackend(msg *Message, bq BackendQueue) error {
	buf := bufferPoolGet()     // 获取缓冲区对象
	defer bufferPoolPut(buf)   // 结束时暂存缓冲区对象
	_, err := msg.WriteTo(buf) // 将数据写入到缓冲区对象中
	if err != nil {
		return err
	}
	return bq.Put(buf.Bytes()) // 将缓冲区对象转化成数据发送到磁盘队列中
}
