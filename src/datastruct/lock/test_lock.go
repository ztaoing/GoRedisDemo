/**
* @Author:zhoutao
* @Date:2021/3/4 上午10:45
* @Desc:
 */

package lock

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

func GOID_test(t testing.T) {
	lm := Locks{}
	size := 10
	var wg sync.WaitGroup
	wg.Add(size)

	for i := 0; i < size; i++ {
		go func(i int) {
			lm.Locks("1", "2")
			println("go : " + strconv.Itoa(GoID()))
			time.Sleep(time.Second)

			println("go: " + strconv.Itoa(GoID()))
			lm.UnLocks("1", "2")
			wg.Done()
		}(i)
	}
	wg.Wait()
}
