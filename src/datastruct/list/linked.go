/**
* @Author:zhoutao
* @Date:2021/3/3 上午10:51
* @Desc:
 */

package list

import "github.com/ztaoing/GoRedisDemo/src/datastruct/utils"

type node struct {
	val  interface{}
	pre  *node
	next *node
}
type LinkedList struct {
	first *node
	last  *node
	size  int
}

func (l *LinkedList) Add(val interface{}) {
	if l == nil {
		panic("list is nil")
	}

	n := &node{
		val: val,
	}
	//空链表
	if l.last == nil {
		l.first = n
		l.last = n
	} else {
		//将node添加到链表的后端
		n.pre = l.last
		//跟新节点的最后一个节点
		l.last = n
	}
	l.size++
}

func (l *LinkedList) Find(index int) *node {
	//二分
	if index < l.size/2 {
		//从前向后
		n := l.first
		for i := 0; i < index; i++ {
			n = n.next
		}
		return n
	} else {
		//从后向前
		n := l.last
		for i := l.size - 1; i > index; i-- {
			n = n.pre
		}
		return n
	}
}

func (l *LinkedList) Get(index int) interface{} {
	if l == nil {
		panic("list is nil")
	}
	if index < 0 || index >= l.size {
		panic("index out of bound")
	}
	return l.Find(index)
}

func (l *LinkedList) Set(index int, val interface{}) {
	if l == nil {
		panic("list is nil")
	}
	if index < 0 || index >= l.size {
		panic("index out of bound")
	}
	n := l.Find(index)
	n.val = val
}

func (l *LinkedList) Insert(index int, val interface{}) {
	if l == nil {
		panic("list is nil")
	}
	if index < 0 || index >= l.size {
		panic("index out of bound")
	}

	if index == l.size {
		l.Add(val)
		return
	} else {
		// list is not empty
		pivot := l.Find(index)
		n := &node{
			val:  val,
			pre:  pivot.pre,
			next: pivot,
		}
		if pivot.pre == nil {
			//空链表
			l.first = n
		} else {
			//将新节点插到pivot前边
			pivot.pre.next = n
		}
		pivot.pre = n
		l.size++
	}
}

func (l *LinkedList) removeNode(n *node) {
	if n.pre == nil {
		l.first = n.next
	} else {
		//将n的前节点与n的后节点相连
		n.pre.next = n.next
	}
	//n 为最后一个节点
	if n.next == nil {
		l.last = n.pre
	} else {
		//将n的后节点的前指针，指向n的前节点
		n.next.pre = n.pre
	}
	n.pre = nil
	n.next = nil
	l.size--
}

func (l *LinkedList) Remove(index int) interface{} {
	if l == nil {
		panic("list is nil")
	}
	if index < 0 || index >= l.size {
		panic("index out of bound")
	}
	n := l.Find(index)
	l.removeNode(n)
	return n.val
}

func (l *LinkedList) RemoveLast() interface{} {
	if l == nil {
		panic("list is nil")
	}
	if l.last == nil {
		// empty list
		return nil
	}
	n := l.last
	l.removeNode(n)
	return n.val
}

func (l *LinkedList) RemoveAllByVal(val interface{}) int {
	if l == nil {
		panic("list is nil")
	}
	n := l.first
	removed := 0
	for n != nil {
		var toRemoveNode *node
		//相等
		if utils.Equals(n.val, val) {
			toRemoveNode = n
		}
		if n.next == nil {
			if toRemoveNode != nil {
				removed++
				l.removeNode(toRemoveNode)
			}
			//尾部
			break
		} else {
			//后移
			n = n.next
		}
		if toRemoveNode != nil {
			removed++
			l.removeNode(toRemoveNode)
		}
	}
	return removed
}

func (l *LinkedList) RemovedByVal(val interface{}, count int) int {
	if l == nil {
		panic("list is nil")
	}
	n := l.first
	removed := 0
	for n != nil {
		var toRemoveNode *node
		if utils.Equals(n.val, val) {
			toRemoveNode = n
		}
		if n.next == nil {
			if toRemoveNode != nil {
				removed++
				l.removeNode(n)
			}
			//tail
			break
		} else {
			n = n.next
		}

		if toRemoveNode != nil {
			removed++
			l.removeNode(toRemoveNode)
		}
		//count
		if removed == count {
			break
		}
	}
	return removed
}

func (l *LinkedList) ReverseRemoveByVal(val interface{}, count int) int {
	if l == nil {
		panic("list is nil")
	}
	//last
	n := l.last
	removed := 0
	for n != nil {
		var toRemoveNode *node
		//相等
		if utils.Equals(n.val, val) {
			toRemoveNode = n
		}
		if n.next == nil {
			if toRemoveNode != nil {
				removed++
				l.removeNode(toRemoveNode)
			}
			//尾部
			break
		} else {
			//后移
			n = n.next
		}
		if toRemoveNode != nil {
			removed++
			l.removeNode(toRemoveNode)
		}
		//count
		if removed == count {
			break
		}
	}
	return removed
}

func (l *LinkedList) Len() int {
	if l == nil {
		panic("list is nil")
	}
	return l.size
}

// todo
func (l *LinkedList) ForEach(consumer func(int, interface{}) bool) {
	if l == nil {
		panic("list is nil")
	}
	n := l.first
	i := 0
	for n != nil {
		//bool
		goNext := consumer(i, n.val)
		if !goNext || n.next == nil {
			break
		} else {
			i++
			n = n.next
		}
	}
}

//todo
func (l *LinkedList) Contains(val interface{}) bool {
	contains := false
	l.ForEach(func(i int, actual interface{}) bool {
		//equal
		if actual == val {
			contains = true
			return false
		}
		return true
	})
	return contains
}

//between start and stop
func (l *LinkedList) Range(start int, stop int) []interface{} {
	if l == nil {
		panic("list is nil")
	}
	if start < 0 || start >= l.size {
		panic("start out of bound")
	}
	if stop < start || stop > l.size {
		panic("stop out of bound")
	}
	sliceSize := stop - start
	slice := make([]interface{}, sliceSize)

	n := l.first
	i := 0
	sliceIndex := 0
	for n != nil {
		if i >= start && i < stop {
			slice[sliceIndex] = n.val
			sliceIndex++
		} else if i >= stop {
			//over
			break
		}
		if n.next == nil {
			//over
			break
		} else {
			i++
			n = n.next
		}
	}
	return slice
}

func Make(vals ...interface{}) *LinkedList {
	l := LinkedList{}
	for _, v := range vals {
		l.Add(v)
	}
	return &l
}

func MakeBytesList(vals ...[]byte) *LinkedList {
	l := LinkedList{}
	for _, v := range vals {
		l.Add(v)
	}
	return &l
}
