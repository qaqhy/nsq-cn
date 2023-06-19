package quantile

import (
	"encoding/json"
	"math"
	"sort"
)

type E2eProcessingLatencyAggregate struct {
	Count       int                  `json:"count"`
	Percentiles []map[string]float64 `json:"percentiles"`
	Topic       string               `json:"topic"`
	Channel     string               `json:"channel"`
	Addr        string               `json:"host"`
}

// UnmarshalJSON 反序列化b中的数据到e对象中
func (e *E2eProcessingLatencyAggregate) UnmarshalJSON(b []byte) error {
	var resp struct {
		Count       int                  `json:"count"`
		Percentiles []map[string]float64 `json:"percentiles"`
		Topic       string               `json:"topic"`
		Channel     string               `json:"channel"`
		Addr        string               `json:"host"`
	}
	err := json.Unmarshal(b, &resp)
	if err != nil {
		return err
	}

	for _, p := range resp.Percentiles {
		p["min"] = p["value"]
		p["max"] = p["value"]
		p["average"] = p["value"]
		p["count"] = float64(resp.Count)
	}

	e.Count = resp.Count
	e.Percentiles = resp.Percentiles
	e.Topic = resp.Topic
	e.Channel = resp.Channel
	e.Addr = resp.Addr

	return nil
}

// Len 用于sort.Sort方法调用的长度算法
func (e *E2eProcessingLatencyAggregate) Len() int { return len(e.Percentiles) }

// Swap 用于sort.Sort方法调用的交换算法
func (e *E2eProcessingLatencyAggregate) Swap(i, j int) {
	e.Percentiles[i], e.Percentiles[j] = e.Percentiles[j], e.Percentiles[i]
}

// Less 用于sort.Sort方法调用的比较算法
func (e *E2eProcessingLatencyAggregate) Less(i, j int) bool {
	return e.Percentiles[i]["percentile"] > e.Percentiles[j]["percentile"]
}

// 通过百分位数的平均化，将e2合并到e中。
func (e *E2eProcessingLatencyAggregate) Add(e2 *E2eProcessingLatencyAggregate) {
	e.Addr = "*"
	p := e.Percentiles
	e.Count += e2.Count
	for _, value := range e2.Percentiles {
		i := -1
		for j, v := range p { // 寻找相同的百分位并设置下标i
			if value["quantile"] == v["quantile"] {
				i = j
				break
			}
		}
		if i == -1 { // e2中新增百分位时追加到e.Percentiles列表末尾
			i = len(p) // 更新下标i
			e.Percentiles = append(p, make(map[string]float64))
			p = e.Percentiles
			p[i]["quantile"] = value["quantile"] // 初始化百分位
		}
		p[i]["max"] = math.Max(value["max"], p[i]["max"]) // 更新最大值
		p[i]["min"] = math.Min(value["max"], p[i]["max"]) // 更新最小值
		p[i]["count"] += value["count"]                   // 更新此百分位统计总值
		if p[i]["count"] == 0 {
			p[i]["average"] = 0
			continue
		}
		delta := value["average"] - p[i]["average"]
		R := delta * value["count"] / p[i]["count"]
		p[i]["average"] = p[i]["average"] + R // 计算平均值
	}
	sort.Sort(e)
}
