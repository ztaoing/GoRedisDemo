/**
* @Author:zhoutao
* @Date:2021/3/2 下午8:55
* @Desc:
 */

package dict

type SimpleDict struct {
	m map[string]interface{}
}

func MakeSimple() *SimpleDict {
	return &SimpleDict{
		m: make(map[string]interface{}),
	}
}

func (s *SimpleDict) Get(key string) (val interface{}, exists bool) {
	val, ok := s.m[key]
	return val, ok
}

func (s *SimpleDict) Len() int {
	if s.m == nil {
		panic("simpleDict is nil")
	}
	return len(s.m)
}

func (s *SimpleDict) Put(key string, val interface{}) int {
	_, exists := s.m[key]
	s.m[key] = val
	if exists {
		return 0
	} else {
		return 1
	}
}

//如果key不存在则设置
func (s *SimpleDict) PutIfAbsent(key string, val interface{}) int {
	_, exists := s.m[key]
	if exists {
		return 0
	} else {
		s.m[key] = val
		return 1
	}
}

//如果key存在则设置
func (s *SimpleDict) PutIfExists(key string, val interface{}) int {
	_, exists := s.m[key]
	if exists {
		s.m[key] = val
		return 1
	} else {
		return 0
	}
}

func (s *SimpleDict) Remove(key string) int {
	_, exists := s.m[key]
	if exists {
		delete(s.m, key)
		return 1
	} else {
		return 0
	}
}

func (s *SimpleDict) Keys() []string {
	result := make([]string, len(s.m))
	i := 0
	for k := range s.m {
		result[i] = k
	}
	return result
}

func (s *SimpleDict) ForEach(consumer Consumer) {
	for k, v := range s.m {
		if !consumer(k, v) {
			break
		}
	}
}

func (s *SimpleDict) RandomKeys(limit int) []string {
	result := make([]string, limit)
	for i := 0; i < limit; i++ {
		for k := range s.m {
			result[i] = k
			break
		}
	}
	return result
}
