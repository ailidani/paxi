package lib

type Set map[interface{}]struct{}

func NewSet() Set {
	return make(map[interface{}]struct{})
}

func (s Set) Add(e interface{}) {
	s[e] = struct{}{}
}

func (s Set) Has(e interface{}) bool {
	_, exists := s[e]
	return exists
}

func (s Set) Remove(e interface{}) {
	delete(s, e)
}

func (s Set) Slice() []interface{} {
	slice := make([]interface{}, 0)
	for e := range s {
		slice = append(slice, e)
	}
	return slice
}

func (s Set) Clear() {
	for k := range s {
		delete(s, k)
	}
}

func (s Set) Clone() Set {
	clone := NewSet()
	for v := range s {
		clone.Add(v)
	}
	return clone
}
