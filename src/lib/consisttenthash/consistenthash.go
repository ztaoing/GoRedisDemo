/**
* @Author:zhoutao
* @Date:2021/2/28 下午3:36
* @Desc: 一致性hash
 */

package consisttenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type HashFunc func(data []byte) uint32

type Map struct {
	hashFunc HashFunc
	replicas int
	keys     []int //sorted
	hashMap  map[int]string
}

func New(replicas int, fn HashFunc) *Map {
	m := &Map{
		replicas: replicas,             //每个物理节点都会有replicas个虚拟节点
		hashFunc: fn,                   //
		hashMap:  make(map[int]string), //虚拟节点的hash值到物理节点的映射
	}
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE

	}
	return m
}

func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}
		for i := 0; i < m.replicas; i++ {
			//使用i+key作为虚拟节点的key，计算虚拟节点的hash值
			hash := int(m.hashFunc([]byte(strconv.Itoa(i) + key)))
			//将虚拟节点添加到换上
			m.keys = append(m.keys, hash)
			//构建 虚拟节点到物理节点的映射关系
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys)
}

func (m *Map) Get(key string) string {
	if m.IsEmpty() {
		return ""
	}
	//根据key的hashTag确定分布
	patitionKey := getPartitionKey(key)
	hash := int(m.hashFunc([]byte(patitionKey)))
	//sort.search 使用二分查找搜索keys中满足m.keys[i] >=hash的最小izhi
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	//若key的hash值大于最后一个虚拟节点的hash值，则sort.search找不到目标,这种情况选择第一个虚拟节点
	if idx == len(m.keys) {
		idx = 0
	}

	//通过虚拟节点获得物理节点
	return m.hashMap[m.keys[idx]]

}
