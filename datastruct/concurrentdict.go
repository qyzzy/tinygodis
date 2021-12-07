package datastruct

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
)

const PRIME32 = uint32(16777619)

type ConcurrentDict struct {
	Table []*Shard
	Count int32
}

type Shard struct {
	M map[string]interface{}
	// RW Lock
	Mutex sync.RWMutex
}

func computeCapacity(param int) int {
	if param <= 16 {
		return 16
	}
	//
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	} else {
		return int(n + 1)
	}
}

// Constructor
func MakeConcurrentDict(shardCount int) *ConcurrentDict {
	shardCount = computeCapacity(shardCount)
	table := make([]*Shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &Shard{
			M: make(map[string]interface{}),
		}
	}
	dict := &ConcurrentDict{
		Table: table,
		Count: 0,
	}
	return dict
}

// Key's hash, FNV algorithm
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= PRIME32
		hash ^= uint32(key[i])
	}
	return hash
}

func (dict *ConcurrentDict) spread(hashCode uint32) uint32 {
	if dict == nil {
		panic("Dict is nil")
	}
	tableSize := uint32(len(dict.Table))
	return (tableSize - 1) & hashCode
}

func (dict *ConcurrentDict) getShard(index uint32) *Shard {
	if dict == nil {
		panic("Dict is nil")
	}
	return dict.Table[index]
}

func (dict *ConcurrentDict) addCount() int32 {
	return atomic.AddInt32(&dict.Count, 1)
}

func (dict *ConcurrentDict) Get(key string) (interface{}, bool) {
	if dict == nil {
		panic("Dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)
	shard.Mutex.RLock()
	defer shard.Mutex.RUnlock()
	val, exists := shard.M[key]
	return val, exists
}

func (dict *ConcurrentDict) Len() int {
	if dict == nil {
		panic("Dict is nil")
	}
	return int(atomic.LoadInt32(&dict.Count))
}

func (dict *ConcurrentDict) Put(key string, val interface{}) int {
	if dict == nil {
		panic("Dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)
	shard.Mutex.Lock()
	defer shard.Mutex.Unlock()
	if _, ok := shard.M[key]; ok {
		shard.M[key] = val
		return 0
	} else {
		shard.M[key] = val
		// Not exists
		dict.addCount()
		return 1
	}
}

func (dict *ConcurrentDict) PutIfAbsent(key string, val interface{}) int {
	if dict == nil {
		panic("Dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)
	shard.Mutex.Lock()
	defer shard.Mutex.Unlock()
	if _, ok := shard.M[key]; ok {
		return 0
	} else {
		shard.M[key] = val
		dict.addCount()
		return 1
	}
}

func (dict *ConcurrentDict) PutIfExists(key string, val interface{}) int {
	if dict == nil {
		panic("Dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)
	shard.Mutex.Lock()
	defer shard.Mutex.Unlock()
	if _, ok := shard.M[key]; ok {
		shard.M[key] = val
		return 1
	} else {
		return 0
	}
}

func (dict *ConcurrentDict) Remove(key string) int {
	if dict == nil {
		panic("Dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	shard := dict.getShard(index)
	shard.Mutex.Lock()
	defer shard.Mutex.Unlock()
	if _, ok := shard.M[key]; ok {
		delete(shard.M, key)
		return 1
	} else {
		return 0
	}
}

func (dict *ConcurrentDict) ForEach(consumer Consumer) {
	if dict == nil {
		panic("Dict is nil")
	}
	for _, shard := range dict.Table {
		for k, v := range shard.M {
			shard.Mutex.RLock()
			continues := consumer(k, v)
			shard.Mutex.RUnlock()
			if !continues {
				return
			}
		}
	}
}

func (dict *ConcurrentDict) Keys() []string {
	keys := make([]string, dict.Len())
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		if i < len(keys) {
			keys[i] = key
			i++
		} else {
			keys = append(keys, key)
		}
		return true
	})
	return keys
}

func (shard *Shard) RandomKey() string {
	if shard == nil {
		panic("Shard is nil")
	}
	shard.Mutex.RLock()
	defer shard.Mutex.RUnlock()
	for k := range shard.M {
		return k
	}
	return ""
}

func (dict *ConcurrentDict) RandomKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}
	shardCount := len(dict.Table)
	result := make([]string, limit)
	for i := 0; i < limit; {
		shard := dict.getShard(uint32(rand.Intn(shardCount)))
		if shard == nil {
			continue
		}
		key := shard.RandomKey()
		if key != "" {
			result[i] = key
			i++
		}
	}
	return result
}

func (dict *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.Keys()
	}
	shardCount := len(dict.Table)
	result := make(map[string]bool)
	for len(result) < limit {
		shardIndex := uint32(rand.Intn(shardCount))
		shard := dict.getShard(shardIndex)
		if shard == nil {
			continue
		}
		key := shard.RandomKey()
		if key != "" {
			result[key] = true
		}
	}
	res := make([]string, limit)
	i := 0
	for k := range result {
		res[i] = k
		i++
	}
	return res
}
