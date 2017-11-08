package cache

import (
	"sync"
	"sort"
)

type Message struct {
	id          int
	handlerName string
	params      map[string]string
	timeStamp   int
}

type Storage struct {
	data map[int]Message
	mx   *sync.RWMutex
}

func NewStorage() *Storage {
	return &Storage{
		data: make(map[int]Message),
		mx:   &sync.RWMutex{},
	}
}

func (s *Storage) Get(timeStamp int) []Message {
	var keys []int
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	var result []Message
	for _, k := range keys {
		if k <= timeStamp {
			result = append(result, s.data[k])
			delete(s.data, k)
		}

	}

	return result
}

func (s *Storage) Add(data Message) {
	s.mx.Lock()
	s.data[data.timeStamp] = data
	s.mx.Unlock()
}
