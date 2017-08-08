package paxi

import "sync"

type MMap struct {
	data map[interface{}]map[interface{}]interface{}
	sync.RWMutex
}

func NewMMap() *MMap {
	return &MMap{
		data: make(map[interface{}]map[interface{}]interface{}),
	}
}

func (m *MMap) keys() []interface{} {
	m.RLock()
	defer m.RUnlock()
	keys := make([]interface{}, len(m.data))
	i := 0
	for key := range m.data {
		keys[i] = key
		i++
	}
	return keys
}

func (m *MMap) secondaryKeys(key interface{}) []interface{} {
	m.RLock()
	defer m.RUnlock()
	keys := make([]interface{}, len(m.data))
	i := 0
	for key := range m.data[key] {
		keys[i] = key
		i++
	}
	return keys
}

func (m *MMap) get(key interface{}, version interface{}) interface{} {
	m.RLock()
	defer m.RUnlock()
	return m.data[key][version]
}

func (m *MMap) put(key interface{}, key2 interface{}, value interface{}) {
	m.Lock()
	defer m.Unlock()
	m.data[key][key2] = value
}
