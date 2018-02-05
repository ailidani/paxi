package lib

import "sync"

// CSet is concurrent set with generic data as interface{}
type CSet struct {
	data map[interface{}]struct{}
	sync.RWMutex
}

func NewCSet() *CSet {
	return &CSet{
		data: make(map[interface{}]struct{}),
	}
}

func (s *CSet) Put(e interface{}) {
	s.Lock()
	defer s.Unlock()
	s.data[e] = struct{}{}
}

// Get returns random element from set
func (s *CSet) Get() interface{} {
	s.RLock()
	defer s.Unlock()
	for e := range s.data {
		return e
	}
	return nil
}

func (s *CSet) Contains(e interface{}) bool {
	s.RLock()
	defer s.RUnlock()
	_, exists := s.data[e]
	return exists
}

func (s *CSet) Remove(e interface{}) {
	s.Lock()
	defer s.Unlock()
	delete(s.data, e)
}

func (s *CSet) Size() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.data)
}

func (s *CSet) Array() []interface{} {
	array := make([]interface{}, 0)
	s.RLock()
	defer s.RUnlock()
	for e := range s.data {
		array = append(array, e)
	}
	return array
}
