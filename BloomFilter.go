package gBloomFilter

import (
	"errors"
	"fmt"
	"github.com/spaolacci/murmur3"
	"math"
	"sync"
)

const (
	mod7		=	1 << 3-1
	bitPerByte	= 8
)

type BloomFilter struct {
	lock 		*sync.RWMutex
	concurrent 	bool
	m 			uint64 //size
	n 			uint64 //inert number
	log2m 		uint64
	k 			uint64 //hash number
	keys 		[]byte
}

func NewBloomFilter(m uint64, k uint64, race bool) *BloomFilter {
	log2 := uint64(math.Ceil(math.Log2(float64(m))))
	filter := &BloomFilter{
		m :		1 << log2,
		log2m: log2,
		k: k,
		keys: make([]byte, 1<<log2),
		concurrent: race,
	}

	if filter.concurrent {
		filter.lock = &sync.RWMutex{}
	}
	return filter
}

func (bf *BloomFilter) location(h uint64) (uint64, uint64) {
	slot := (h/bitPerByte) & (bf.m - 1)
	mod := h &mod7
	return slot, mod
}

func (bf *BloomFilter) Add(data []byte) *BloomFilter {
	if bf.concurrent {
		bf.lock.Lock()
		defer bf.lock.Unlock()
	}
	h := baseHash(data)
	for i := uint64(0); i < bf.k; i++ {
		loc := location(h, i)
		slot, mod := bf.location(loc)
		bf.keys[slot] |= 1 << mod
	}
	bf.n++
	return bf
}


func (bf *BloomFilter) Check(data []byte) bool {
	if bf.concurrent {
		bf.lock.RLock()
		defer bf.lock.RUnlock()
	}
	h := baseHash(data)
	for i := uint64(0); i < bf.k; i++ {
		loc := location(h, i)
		slot, mod := bf.location(loc)
		if bf.keys[slot]&(1<<mod) == 0 {
			return false
		}
	}
	return true
}

// AddString adds string to filter
func (bf *BloomFilter) AddString(s string) *BloomFilter {
	data := str2Bytes(s)
	return bf.Add(data)
}

// TestString if string may exist in filter
func (bf *BloomFilter) CheckString(s string) bool {
	data := str2Bytes(s)
	return bf.Check(data)
}

// AddUInt16 adds uint16 to filter
func (bf *BloomFilter) AddUInt16(num uint16) *BloomFilter {
	data := uint16ToBytes(num)
	return bf.Add(data)
}

// TestUInt16 checks if uint16 is in filter
func (bf *BloomFilter) CheckUInt16(num uint16) bool {
	data := uint16ToBytes(num)
	return bf.Check(data)
}

// AddUInt32 adds uint32 to filter
func (bf *BloomFilter) AddUInt32(num uint32) *BloomFilter {
	data := uint32ToBytes(num)
	return bf.Add(data)
}

// TestUInt32 checks if uint32 is in filter
func (bf *BloomFilter) CheckUInt32(num uint32) bool {
	data := uint32ToBytes(num)
	return bf.Check(data)
}

// AddUInt64 adds uint64 to filter
func (bf *BloomFilter) AddUInt64(num uint64) *BloomFilter {
	data := uint64ToBytes(num)
	return bf.Add(data)
}

// TestUInt64 checks if uint64 is in filter
func (bf *BloomFilter) CheckUInt64(num uint64) bool {
	data := uint64ToBytes(num)
	return bf.Check(data)
}

// AddBatch add data array
func (bf *BloomFilter) AddBatch(dataArr [][]byte) *BloomFilter {
	if bf.concurrent {
		bf.lock.Lock()
		defer bf.lock.Unlock()
	}
	for i := 0; i < len(dataArr); i++ {
		data := dataArr[i]
		h := baseHash(data)
		for i := uint64(0); i < bf.k; i++ {
			loc := location(h, i)
			slot, mod := bf.location(loc)
			bf.keys[slot] |= 1 << mod
		}
		bf.n++
	}
	return bf
}

// AddUint16Batch adds uint16 array
func (bf *BloomFilter) AddUint16Batch(numArr []uint16) *BloomFilter {
	data := make([][]byte, 0, len(numArr))
	for i := 0; i < len(numArr); i++ {
		byteArr := uint16ToBytes(numArr[i])
		data = append(data, byteArr)
	}
	return bf.AddBatch(data)
}

// AddUint32Batch adds uint32 array
func (bf *BloomFilter) AddUint32Batch(numArr []uint32) *BloomFilter {
	data := make([][]byte, 0, len(numArr))
	for i := 0; i < len(numArr); i++ {
		byteArr := uint32ToBytes(numArr[i])
		data = append(data, byteArr)
	}
	return bf.AddBatch(data)
}

// AddUin64Batch  adds uint64 array
func (bf *BloomFilter) AddUin64Batch(numArr []uint64) *BloomFilter {
	data := make([][]byte, 0, len(numArr))
	for i := 0; i < len(numArr); i++ {
		byteArr := uint64ToBytes(numArr[i])
		data = append(data, byteArr)
	}
	return bf.AddBatch(data)
}

// location returns the ith hashed location using the four base hash values
func location(h []uint64, i uint64) uint64 {
	// return h[ii%2] + ii*h[2+(((ii+(ii%2))%4)/2)]
	return h[i&1] + i*h[2+(((i+(i&1))&3)/2)]
}

// baseHash returns the murmur3 128-bit hash
func baseHash(data []byte) []uint64 {
	a1 := []byte{1} // to grab another bit of data
	hasher := murmur3.New128()
	hasher.Write(data) // #nosec
	v1, v2 := hasher.Sum128()
	hasher.Write(a1) // #nosec
	v3, v4 := hasher.Sum128()
	return []uint64{
		v1, v2, v3, v4,
	}
}

// Reset reset the bits to zero used in filter
func (bf *BloomFilter) Reset() {
	if bf.concurrent {
		bf.lock.Lock()
		defer bf.lock.Unlock()
	}
	for i := 0; i < len(bf.keys); i++ {
		bf.keys[i] &= 0
	}
	bf.n = 0
}

// MergeInPlace merges another filter into current one
func (bf *BloomFilter) MergeInPlace(g *BloomFilter) error {
	if bf.m != g.m {
		return fmt.Errorf("m's don't match: %d != %d", bf.m, g.m)
	}

	if bf.k != g.k {
		return fmt.Errorf("k's don't match: %d != %d", bf.m, g.m)
	}
	if g.concurrent {
		return errors.New("merging concurrent filter is not support")
	}

	if bf.concurrent {
		bf.lock.Lock()
		defer bf.lock.Unlock()
	}
	for i := 0; i < len(bf.keys); i++ {
		bf.keys[i] |= g.keys[i]
	}
	return nil
}

// Cap return the size of bits
func (bf *BloomFilter) Cap() uint64 {
	if bf.concurrent {
		bf.lock.RLock()
		defer bf.lock.RUnlock()
	}
	return bf.m
}

// KeySize return  count of inserted element
func (bf *BloomFilter) KeySize() uint64 {
	if bf.concurrent {
		bf.lock.RLock()
		defer bf.lock.RUnlock()
	}
	return bf.n
}

// FalsePositiveRate returns (1 - e^(-kn/m))^k
func (bf *BloomFilter) FalsePositiveRate() float64 {
	if bf.concurrent {
		bf.lock.RLock()
		defer bf.lock.RUnlock()
	}
	expoInner := -(float64)(bf.k*bf.n) / float64(bf.m)
	rate := math.Pow(1-math.Pow(math.E, expoInner), float64(bf.k))
	return rate
}
