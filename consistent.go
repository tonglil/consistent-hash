package consistent

import (
	"hash/crc32"
	"sort"
	"sync"
)

// Inspired by:
// https://github.com/golang/groupcache/blob/master/consistenthash/consistenthash.go
// https://github.com/stathat/consistent/blob/master/consistent.go

type Hash func(data []byte) uint32

type Consistent struct {
	sync.RWMutex
	hash    Hash
	keys    []int // Sorted
	hashMap map[int]string
}

func New(fn Hash) *Consistent {
	m := &Consistent{
		hash:    fn,
		hashMap: make(map[int]string),
	}

	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}

	return m
}

// Returns true if there are no items available.
func (m *Consistent) IsEmpty() bool {
	m.RLock()
	defer m.RUnlock()
	return len(m.keys) == 0
}

// Hash a key.
func (m *Consistent) Hash(key string) int {
	return int(m.hash([]byte(key)))
}

// Add a key to the hash.
func (m *Consistent) Add(key string) int {
	hash := m.Hash(key)

	m.Lock()
	defer m.Unlock()
	if _, ok := m.hashMap[hash]; !ok {
		// Do not add another key to the sorted index if it already exists
		m.keys = append(m.keys, hash)
		sort.Ints(m.keys)
	}

	m.hashMap[hash] = key

	return hash
}

// Remove a key from the hash.
func (m *Consistent) Remove(key string) {
	hash := m.Hash(key)

	m.Lock()
	defer m.Unlock()
	// Remove hash from m.keys
	i := sort.SearchInts(m.keys, hash)
	if i < len(m.keys) && m.keys[i] == hash {
		m.keys = append(m.keys[:i], m.keys[i+1:]...)
	}

	// Remove hash from hashMap
	delete(m.hashMap, hash)

	sort.Ints(m.keys)
}

// Check if a key is in the hash.
func (m *Consistent) Has(key string) bool {
	hash := m.Hash(key)

	m.Lock()
	defer m.Unlock()
	_, ok := m.hashMap[hash]

	return ok
}

// Get the item in the hash the provided key is in the range of.
func (m *Consistent) Get(key string) string {
	return m.Next(key)
}

// TODO:
// This method should be refactored to use "next()" and return replicas only, excluding current node
// Return the current next n-1 nodes that the key belongs on
//func (m *Consistent) GetReplicas(key string, count int) []string {
//if m.IsEmpty() {
//return nil
//}

//locations := make([]string, count)
//hash := m.Hash(key)

//m.RLock()
//defer m.RUnlock()
//index := m.prev(hash)

//for i := 0; i < count; i++ {
//locations[i] = m.hashMap[index]
//index = m.next(index)
//}

//return locations
//}

// Get the next item in the hash to the provided key.
func (m *Consistent) Next(key string) string {
	if m.IsEmpty() {
		return ""
	}

	hash := m.Hash(key)

	m.RLock()
	defer m.RUnlock()

	index := m.next(hash)
	return m.hashMap[index]
}

// Get the next N items in the hash to the provided key.
func (m *Consistent) NextN(key string, count int) []string {
	if m.IsEmpty() {
		return nil
	}

	locations := make([]string, count)
	hash := m.Hash(key)

	m.RLock()
	defer m.RUnlock()

	for i := 0; i <= count; i++ {
		hash = m.next(hash)
		locations[i] = m.hashMap[hash]
	}

	return locations
}

// Get the previous N items in the hash to the provided key.
func (m *Consistent) PrevN(key string, count int) []string {
	if m.IsEmpty() {
		return nil
	}

	locations := make([]string, count)
	hash := m.Hash(key)

	m.RLock()
	defer m.RUnlock()
	index := m.prev(hash)

	for i := 0; i < count; i++ {
		locations[i] = m.hashMap[index]
		index = m.prev(index - 1)
	}

	return locations
}

// Get the range of hash keys to the provided item.
func (m *Consistent) Range(host string) (int, int) {
	if m.IsEmpty() {
		return 0, 0
	}

	to := m.Hash(host)
	from := m.prev(to-1) + 1

	return from, to
}

// Internal operation, not thread safe, need to be R-locked
func (m *Consistent) prev(hash int) int {
	rev := make([]int, len(m.keys))
	copy(rev, m.keys)
	sort.Sort(sort.Reverse(sort.IntSlice(rev)))

	i := sort.Search(len(rev), func(i int) bool { return rev[i] <= hash })

	if i == len(rev) {
		i = 0
	}

	return rev[i]
}

// Internal operation, not thread safe, need to be R-locked
func (m *Consistent) next(hash int) int {
	i := sort.Search(len(m.keys), func(i int) bool { return m.keys[i] > hash })

	if i == len(m.keys) {
		i = 0
	}

	return m.keys[i]
}
