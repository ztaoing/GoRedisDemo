/**
* @Author:zhoutao
* @Date:2021/2/28 下午12:56
* @Desc:
 */

package wait

import (
	"sync"
	"time"
)

type Wait struct {
	wg sync.WaitGroup
}

func (w *Wait) Add(num int) {
	w.wg.Add(num)
}

func (w *Wait) Done() {
	w.wg.Done()
}

func (w *Wait) wait() {
	w.wg.Wait()
}

func (w *Wait) WaitWithTimeout(timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		w.wg.Wait()
		c <- struct{}{}
	}()

	select {
	case <-c:
		//正常关闭
		return false
	case <-time.After(timeout):
		//超时
		return true
	}
}
