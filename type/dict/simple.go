package dict

type SimpleDict struct {
	M map[string]interface{}
}

func (dict *SimpleDict) Get(key string) (interface{}, bool) {
	val, ok := dict.M[key]
	return val, ok
}

func (dict *SimpleDict) Len() int {
	if dict.M == nil {
		panic("Dict is nil")
	}
	return len(dict.M)
}

func (dict *SimpleDict) Put(key string, val interface{}) int {
	_, existed := dict.M[key]
	dict.M[key] = val
	if existed {
		return 0
	} else {
		return 1
	}
}

func (dict *SimpleDict) PutIfAbsent(key string, val interface{}) int {
	_, existed := dict.M[key]
	if existed {
		return 0
	} else {
		dict.M[key] = val
		return 1
	}
}

func (dict *SimpleDict) PutIfExists(key string, val interface{}) int {
	_, existed := dict.M[key]
	if !existed {
		return 1
	} else {
		dict.M[key] = val
		return 0
	}
}

func (dict *SimpleDict) Remove(key string) int {
	_, existed := dict.M[key]
	delete(dict.M, key)
	if existed {
		return 1
	} else {
		return 0
	}
}

func (dict *SimpleDict) ForEach(consumer Consumer) {
	for k, v := range dict.M {
		//
		if !consumer(k, v) {
			break
		}
	}
}

// All keys
func (dict *SimpleDict) Keys() []string {
	result := make([]string, len(dict.M))
	i := 0
	for k := range dict.M {
		result[i] = k
		i++
	}
	return result
}

func (dict *SimpleDict) RandomKeys(limit int) []string {
	result := make([]string, limit)
	for i := 0; i < limit; i++ {
		for k := range dict.M {
			result[i] = k
			break
		}
	}
	return result
}

// Distinct
func (dict *SimpleDict) RandomDistinctKeys(limit int) []string {
	size := len(dict.M)
	if size > limit {
		size = limit
	}
	result := make([]string, size)
	i := 0
	for k := range dict.M {
		if i == limit {
			break
		}
		result[i] = k
		i++
	}
	return result
}
