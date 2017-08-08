package paxi

import "sync"

type CMap struct {
	data map[interface{}]interface{}
	sync.RWMutex
}

func NewCMap() *CMap {
	return &CMap{
		data: make(map[interface{}]interface{}),
	}
}

func (c *CMap) get(key interface{}) interface{} {
	c.RLock()
	defer c.RUnlock()
	return c.data[key]
}

func (c *CMap) set(key, value interface{}) {
	c.Lock()
	defer c.Unlock()
	c.data[key] = value
}

func (c *CMap) exist(key interface{}) bool {
	c.RLock()
	defer c.RUnlock()
	_, exist := c.data[key]
	return exist
}

func (c *CMap) keys() []interface{} {
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
