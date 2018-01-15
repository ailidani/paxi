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
	slice := make([]interface{}, len(s))
	for e, _ := range s {
		slice = append(slice, e)
	}
	return slice
}
