/**
* @Author:zhoutao
* @Date:2021/3/1 下午1:40
* @Desc:
 */

package idgenerator

import (
	"hash/fnv"
	"log"
	"sync"
	"time"
)

const (
	//bit means
	workerIDBits     int64 = 5
	dataCenterIDBits int64 = 5
	sequenceBits     int64 = 12

	maxWorkerID     int64 = -1 ^ (-1 << uint64(workerIDBits))
	maxDataCenterID int64 = -1 ^ (-1 << uint64(dataCenterIDBits))
	maxSequence     int64 = -1 ^ (-1 << uint64(sequenceBits))

	timeLeft uint8 = 22
	dataLeft uint8 = 17
	workLeft uint8 = 12
	//开始的时间戳
	twepoch int64 = 1525705533000
)

type IDGenerator struct {
	mu           *sync.Mutex
	lastStamp    int64
	workerID     int64
	dataCenterID int64
	sequence     int64
}

func MakeGenerator(cluster string, node string) *IDGenerator {
	fnv64 := fnv.New64()
	_, _ = fnv64.Write([]byte(cluster))
	dataCenterID := int64(fnv64.Sum64())

	fnv64.Reset()
	_, _ = fnv64.Write([]byte(node))
	workerID := int64(fnv64.Sum64())

	return &IDGenerator{
		mu:           &sync.Mutex{},
		lastStamp:    -1,
		workerID:     workerID,
		dataCenterID: dataCenterID,
		sequence:     1,
	}
}

func (i *IDGenerator) getCurrentTime() int64 {
	return time.Now().UnixNano() / 1e6
}

func (i *IDGenerator) NextID() int64 {
	i.mu.Lock()
	defer i.mu.Unlock()

	timestamp := i.getCurrentTime()
	if timestamp < i.lastStamp {
		log.Fatal("generate id error because of time")
	}

	if i.lastStamp == timestamp {
		i.sequence = (i.sequence + 1) & maxSequence
		if i.sequence == 0 {
			for timestamp <= i.lastStamp {
				timestamp = i.getCurrentTime()
			}
		}
	} else {
		i.sequence = 0
	}
	// ?
	return (timestamp-twepoch)<<timeLeft | (i.dataCenterID << dataLeft) | (i.workerID << workLeft) | i.sequence
}

func (i *IDGenerator) tillNextMillis() int64 {
	timestamp := i.getCurrentTime()
	if timestamp <= i.lastStamp {
		timestamp = i.getCurrentTime()
	}
	return timestamp
}
