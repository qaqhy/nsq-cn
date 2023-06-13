package nsqd

type inFlightPqueue []*Message // 重构小根堆算法为了优化节省container/heap在使用中装箱和接口类型断言操作造成的性能相关成本。

// newInFlightPqueue 生成指定初始容量的堆inFlightPqueue列表对象
func newInFlightPqueue(capacity int) inFlightPqueue {
	return make(inFlightPqueue, 0, capacity)
}

// Swap 交换下标i,j中的数据内容
func (pq inFlightPqueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// Push 存储数据到堆中 O(logN)
func (pq *inFlightPqueue) Push(x *Message) {
	n := len(*pq)
	c := cap(*pq)
	if n+1 > c { // 当新加入数据不满足当前长度时双倍扩容inFlightPqueue列表
		npq := make(inFlightPqueue, n, c*2)
		copy(npq, *pq) // 拷贝所有数据到npq对象中
		*pq = npq      // pq地址引用为npq地址
	}
	*pq = (*pq)[0 : n+1] // 增加一个列表长度
	x.index = n
	(*pq)[n] = x // 列表末尾增加此元素
	pq.up(n)     // 末尾数据上浮
}

// Pop 删除并取出堆顶数据 O(logN)
func (pq *inFlightPqueue) Pop() *Message {
	n := len(*pq)
	c := cap(*pq)
	pq.Swap(0, n-1)          // 堆顶数据交换到队列末尾等待移除
	pq.down(0, n-1)          // 将交换到堆顶的数据下沉到合适的队列位置
	if n < (c/2) && c > 25 { // 列表容量大于25且大于数组长度两倍的情况下,缩减inFlightPqueue列表对象容量
		npq := make(inFlightPqueue, n, c/2) // 申请原容量的一半大小
		copy(npq, *pq)                      // 拷贝所有数据到npq对象中
		*pq = npq                           // pq地址引用为npq地址
	}
	x := (*pq)[n-1]      // 取出队列末尾的数据
	x.index = -1         // 修改索引为-1,代表堆已做完操作
	*pq = (*pq)[0 : n-1] // 删掉队列末尾元素
	return x
}

// Remove 删除并返回指定坐标下的数据 O(logN)
func (pq *inFlightPqueue) Remove(i int) *Message {
	n := len(*pq)
	if n-1 != i { // 删除数据不是队列最后一个元素时,将更新队列前面的数据
		pq.Swap(i, n-1) // 指定坐标数据交换到队列末尾等待移除
		pq.down(i, n-1) // 将交换到指定坐标下的数据下沉到合适的队列位置
		pq.up(i)        // 将交换到指定坐标下的数据上浮到合适的队列位置
	}
	x := (*pq)[n-1]      // 取出队列末尾的数据
	x.index = -1         // 修改索引为-1,代表堆已做完操作
	*pq = (*pq)[0 : n-1] // 删掉队列末尾元素
	return x
}

// PeekAndShift 判断是否存在满足max的最小数据,满足时获取对应数据,不满足则返回还需等待多少纳秒
func (pq *inFlightPqueue) PeekAndShift(max int64) (*Message, int64) {
	if len(*pq) == 0 { // 堆大小为0,直接退出
		return nil, 0
	}

	x := (*pq)[0]    // 取出堆顶数据,小根堆值最小
	if x.pri > max { // 不满足要求的最小值,不返回数据对象
		return nil, x.pri - max
	}
	pq.Pop() // 满足要求的最小值,将堆顶数据弹出

	return x, 0
}

// up 数据上浮 O(logN)
func (pq *inFlightPqueue) up(j int) {
	for {
		i := (j - 1) / 2                            // 父节点
		if i == j || (*pq)[j].pri >= (*pq)[i].pri { // 子节点不是根节点或子节点优先级高于父节点则满足小根堆结构(跳过排序)
			break
		}
		pq.Swap(i, j) // 交换父节点i和当前节点j的数据
		j = i
	}
}

// down 数据下沉 O(logN)
func (pq *inFlightPqueue) down(i, n int) {
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // 下标超出数组范围或int范围溢出时直接退出
			break
		}
		j := j1                                                     // 左分支
		if j2 := j1 + 1; j2 < n && (*pq)[j1].pri >= (*pq)[j2].pri { // 取左右分支中最小数据(满足小根堆)
			j = j2 // = 2*i + 2  // 右分支
		}
		if (*pq)[j].pri >= (*pq)[i].pri { // 最小子节点数比当前节点数大则退出排序
			break
		}
		pq.Swap(i, j) // 交换当前节点i和子节点j的数据
		i = j
	}
}
