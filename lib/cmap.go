package lib

import "sync"

// CMap is concurrent map with generic key and value as interface{}
type CMap struct {
	data map[interface{}]interface{}
	sync.RWMutex
}

// NewCMap make new CMap
func NewCMap() *CMap {
	return &CMap{
		data: make(map[interface{}]interface{}),
	}
}

func (c *CMap) Get(key interface{}) interface{} {
	c.RLock()
	defer c.RUnlock()
	return c.data[key]
}

func (c *CMap) Put(key, value interface{}) {
	c.Lock()
	defer c.Unlock()
	c.data[key] = value
}

func (c *CMap) Contains(key interface{}) bool {
	c.RLock()
	defer c.RUnlock()
	_, exist := c.data[key]
	return exist
}

func (c *CMap) Size() int {
	c.RLock()
	defer c.RUnlock()
	return len(c.data)
}

func (c *CMap) Keys() []interface{} {
	c.RLock()
	defer c.RUnlock()
	keys := make([]interface{}, len(c.data))
	i := 0
	for key := range c.data {
		keys[i] = key
		i++
	}
	return keys
}
