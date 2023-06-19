package quantile

import (
	"strings"
	"sync"
	"time"

	"github.com/bmizerany/perks/quantile"
	"github.com/nsqio/nsq/internal/stringy"
)

type Result struct {
	Count       int                  `json:"count"`       // 统计数据总数
	Percentiles []map[string]float64 `json:"percentiles"` // 百分位信息及统计结果信息[quantile:百分位;value:百分位统计占比]
}

func (r *Result) String() string {
	var s []string
	for _, item := range r.Percentiles {
		s = append(s, stringy.NanoSecondToHuman(item["value"]))
	}
	return strings.Join(s, ", ")
}

type Quantile struct {
	sync.Mutex
	streams        [2]quantile.Stream // 设计两个流的数组对象,一个是当前正在使用的流和一个前窗口时间下使用的流
	currentIndex   uint8
	lastMoveWindow time.Time
	currentStream  *quantile.Stream

	Percentiles    []float64
	MoveWindowTime time.Duration
}

// New 初始化链路追踪对象
func New(WindowTime time.Duration, Percentiles []float64) *Quantile {
	q := Quantile{
		currentIndex:   0,
		lastMoveWindow: time.Now(),     // 初始化最后窗口移动时间
		MoveWindowTime: WindowTime / 2, // 设置移动窗口过期时间限制(WindowTime/2)
		Percentiles:    Percentiles,    // 设置计算百分位列表
	}
	for i := 0; i < 2; i++ { // 根据百分位列表初始化两个流对象
		q.streams[i] = *quantile.NewTargeted(Percentiles...)
	}
	q.currentStream = &q.streams[0] // 初始化当前流对象
	return &q
}

// Result 链路追踪结果返回
func (q *Quantile) Result() *Result {
	if q == nil {
		return &Result{}
	}
	queryHandler := q.QueryHandler()
	result := Result{
		Count:       queryHandler.Count(),
		Percentiles: make([]map[string]float64, len(q.Percentiles)),
	}
	for i, p := range q.Percentiles {
		value := queryHandler.Query(p)
		result.Percentiles[i] = map[string]float64{"quantile": p, "value": value}
	}
	return &result
}

// Insert 根据插入时间戳计算此数据是否过期
func (q *Quantile) Insert(msgStartTime int64) {
	q.Lock()

	now := time.Now()
	for q.IsDataStale(now) {
		q.moveWindow()
	}

	// 插入过期时间信息到当前流中
	q.currentStream.Insert(float64(now.UnixNano() - msgStartTime))
	q.Unlock()
}

// QueryHandler 查询流数据对象合并两个流中的数据可以较精确的统计出链路跟踪情况
func (q *Quantile) QueryHandler() *quantile.Stream {
	q.Lock()
	now := time.Now()
	for q.IsDataStale(now) {
		q.moveWindow()
	}

	// 初始化合并统计对象,合并两个流中的数据统计结果
	merged := quantile.NewTargeted(q.Percentiles...)
	merged.Merge(q.streams[0].Samples())
	merged.Merge(q.streams[1].Samples())
	q.Unlock()
	return merged
}

// IsDataStale 判断数据是否过期(当前时间是否在数据截至时间之后)
func (q *Quantile) IsDataStale(now time.Time) bool {
	return now.After(q.lastMoveWindow.Add(q.MoveWindowTime))
}

// Merge 合并链路追踪them中的数据到q对象中
func (q *Quantile) Merge(them *Quantile) {
	q.Lock()
	them.Lock()
	iUs := q.currentIndex
	iThem := them.currentIndex

	q.streams[iUs].Merge(them.streams[iThem].Samples())

	iUs ^= 0x1
	iThem ^= 0x1
	q.streams[iUs].Merge(them.streams[iThem].Samples())

	if q.lastMoveWindow.Before(them.lastMoveWindow) { // 如果q的窗口过期时间在them的窗口过期时间之前则更新them的窗口过期时间到q中
		q.lastMoveWindow = them.lastMoveWindow
	}
	q.Unlock()
	them.Unlock()
}

// moveWindow 切换窗口设置过期时间并重置统计
func (q *Quantile) moveWindow() {
	q.currentIndex ^= 0x1 // 切换下标为另一个下标(0->1或1->0)
	q.currentStream = &q.streams[q.currentIndex]
	q.lastMoveWindow = q.lastMoveWindow.Add(q.MoveWindowTime) // 更新窗口移动过期时间
	q.currentStream.Reset()                                   // 重置统计
}
