package storage

type Index struct {
	data map[string]int64
}

func NewIndex() *Index {
	return &Index{
		data: make(map[string]int64),
	}
}

func (i *Index) Set(key string, value int64) {
	i.data[key] = value
}

func (i *Index) Get(key string) (int64, bool) {
	value, ok := i.data[key]
	return value, ok
}

func (i *Index) Remove(key string) {
	delete(i.data, key)
}
