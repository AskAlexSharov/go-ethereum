// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// memory storage layer for the package blockhash

package storage

import lru "github.com/hashicorp/golang-lru"

type MemStore struct {
	cache    *lru.Cache
	disabled bool
}

//NewMemStore is instantiating a MemStore cache. We are keeping a record of all outgoing requests for chunks, that
//should later be delivered by peer nodes, in the `requests` LRU cache. We are also keeping all frequently requested
//chunks in the `cache` LRU cache.
//
//`requests` LRU cache capacity should ideally never be reached, this is why for the time being it should be initialised
//with the same value as the LDBStore capacity.
func NewMemStore(params *StoreParams, _ *LDBStore) (m *MemStore) {
	if params.CacheCapacity == 0 {
		return &MemStore{
			disabled: true,
		}
	}

	onEvicted := func(key interface{}, value interface{}) {
		v := value.(*Chunk)
		<-v.dbStoredC
	}
	c, err := lru.NewWithEvict(int(params.CacheCapacity), onEvicted)
	if err != nil {
		panic(err)
	}

	return &MemStore{
		cache: c,
	}
}

func (m *MemStore) Get(key Address) (*Chunk, error) {
	if m.disabled {
		return nil, ErrChunkNotFound
	}

	c, ok := m.cache.Get(string(key))
	if !ok {
		return nil, ErrChunkNotFound
	}
	return c.(*Chunk), nil
}

func (m *MemStore) Put(c *Chunk) {
	if m.disabled {
		return
	}

	m.cache.Add(string(c.Address), c)
}

func (m *MemStore) setCapacity(n int) {
	if n <= 0 {
		m.disabled = true
	} else {
		onEvicted := func(key interface{}, value interface{}) {
			v := value.(*Chunk)
			v.WaitToStore()
		}
		c, err := lru.NewWithEvict(n, onEvicted)
		if err != nil {
			panic(err)
		}

		*m = MemStore{
			cache: c,
		}
	}
}

func (s *MemStore) Close() {}
