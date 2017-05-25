// Package gostore is an in-memory key/value store
package gostore

import (
	"fmt"
	"log"
	"time"
)

// Store is the interface to the in-memory store
type Store interface {

	// Init initializes the store
	Init()

	// Close stops all internal goroutines
	Close()

	// Put saves the item in the store given an optional expiry duration.
	Put(item *Item, d time.Duration) error

	// Get returns the item given the key
	Get(key string) (item *Item, found bool, err error)

	// Del deletes the item for the key
	Del(key string) error

	// ListPush adds the item to the list of items
	ListPush(key string, value *Item) error

	// ListGet returns the list of items given a key
	ListGet(key string) (items []*Item, found bool, err error)

	// ListDel deletes the item from the list
	ListDel(key string, value *Item) error

	// OnItemDidExpire adds the callback function to the list off callback functions
	// called when an item expires
	OnItemDidExpire(func(item *Item))

	// OnListDidChange adds a callback to change in list
	OnListDidChange(func(key string, items []*Item))
}

// NewStore returns a new instance of Store
func NewStore() Store {
	s := &store{}
	return s
}

// Store implements a key/value in-memory storage
type store struct {
	ls *listStore
	kv *kvStore
}

func (s *store) Init() {
	s.ls = newListStore()
	s.ls.init()
	s.kv = newKVStore()
	s.kv.init()
}

func (s *store) Close() {
	if s.ls != nil {
		s.ls.closeStore()
	}
	if s.kv != nil {
		s.kv.closeStore()
	}
}

func (s *store) Put(item *Item, d time.Duration) error {
	if s.kv == nil {
		log.Printf("ERROR: Init must be called first")
		return fmt.Errorf("ERROR: Init must be called first")
	}
	return s.kv.put(item, d)
}

func (s *store) Get(key string) (item *Item, found bool, err error) {
	if s.kv == nil {
		log.Printf("ERROR: Init must be called first")
		return nil, false, fmt.Errorf("ERROR: Init must be called first")
	}
	return s.kv.getItem(key)
}

func (s *store) Del(key string) error {
	if s.kv == nil {
		log.Printf("ERROR: Init must be called first")
		return fmt.Errorf("ERROR: Init must be called first")
	}
	return s.kv.delItem(key)
}

func (s *store) ListPush(key string, value *Item) error {
	if s.ls == nil {
		log.Printf("ERROR: Init must be called first")
		return fmt.Errorf("ERROR: Init must be called first")
	}
	return s.ls.listPush(key, value)
}

func (s *store) ListDel(key string, value *Item) error {
	if s.ls == nil {
		log.Printf("ERROR: Init must be called first")
		return fmt.Errorf("ERROR: Init must be called first")
	}
	return s.ls.listDel(key, value)
}

func (s *store) ListGet(key string) ([]*Item, bool, error) {
	if s.ls == nil {
		log.Printf("ERROR: Init must be called first")
		return nil, false, fmt.Errorf("ERROR: Init must be called first")
	}
	return s.ls.listGet(key)
}

func (s *store) OnItemDidExpire(cb func(item *Item)) {
	if s.kv != nil {
		s.kv.onItemDidExpire(cb)
	}
}

func (s *store) OnListDidChange(cb func(string, []*Item)) {
	if s.ls != nil {
		s.ls.onListDidChange(cb)
	}
}
