/**
* @Author:zhoutao
* @Date:2021/3/3 下午1:00
* @Desc:
 */

package set

import "github.com/ztaoing/GoRedisDemo/src/datastruct/dict"

type Set struct {
	dict dict.Dict
}

func Make() *Set {
	return &Set{
		dict: dict.MakeSimple(),
	}
}

func MakeFromVals(members ...string) *Set {
	set := &Set{
		dict: dict.MakeConcurrent(len(members)),
	}
	for _, member := range members {
		set.Add(member)
	}
	return set
}

func (s *Set) Add(val string) int {
	return s.dict.Put(val, true)
}

func (s *Set) Remove(val string) int {
	return s.dict.Remove(val)
}

func (s *Set) Has(val string) bool {
	_, exists := s.dict.Get(val)
	return exists
}

func (s *Set) Len() int {
	return s.dict.Len()
}

func (s *Set) ToSlice() []string {
	slice := make([]string, s.Len())
	i := 0
	s.dict.ForEach(func(key string, val interface{}) bool {
		if i < len(slice) {
			slice[i] = key
		} else {
			slice = append(slice, key)
		}
		i++
		return true
	})
	return slice
}

func (s *Set) ForEach(consumer func(member string) bool) {
	s.dict.ForEach(func(key string, val interface{}) bool {
		return consumer(key)
	})
}

//交集
func (s *Set) Intersect(another *Set) *Set {
	if s == nil {
		panic("set is nil")
	}
	//make set
	result := Make()
	another.ForEach(func(member string) bool {
		if s.Has(member) {
			result.Add(member)
		}
		return true
	})
	return result
}

//并集
func (s *Set) Union(another *Set) *Set {
	if s == nil {
		panic("set is nil")
	}
	result := Make()
	another.ForEach(func(member string) bool {
		result.Add(member)
		return true
	})
	s.ForEach(func(member string) bool {
		result.Add(member)
		return true
	})
	return result
}

//差集
func (s *Set) Diff(another *Set) *Set {
	if s == nil {
		panic("set is nil")
	}
	result := Make()
	s.ForEach(func(member string) bool {
		if !another.Has(member) {
			result.Add(member)
		}
		return true
	})
	return result
}

// random keys
func (s *Set) RandomMembers(limit int) []string {
	return s.dict.RandomKeys(limit)
}

// random distinct keys
func (s *Set) RandomDistinctMembers(limit int) []string {
	return s.dict.RandomDistinctKeys(limit)
}
