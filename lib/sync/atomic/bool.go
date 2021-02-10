/**
* @Author:zhoutao
* @Date:2021/1/21 下午3:37
* @Desc:
 */

package atomic

import "sync/atomic"

type AtomicBool uint32

func (a *AtomicBool) Get() bool {
	return atomic.LoadUint32((*uint32)(a)) != 0
}

func (a *AtomicBool) Set(v bool) {
	if v {
		atomic.StoreUint32((*uint32)(a), 1)
	} else {
		atomic.StoreUint32((*uint32)(a), 0)
	}
}
