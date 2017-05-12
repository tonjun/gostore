// Package gostore is an in-memory key/value store
package gostore

import (
	"fmt"
	"log"
	"time"

	"github.com/google/btree"
)

type Store interface {
	Init()
	Close()
	Set(key string, value *Item, d time.Duration) error
	Get(key string) (item *Item, found bool, err error)
	Del(key string) error
	ListPush(key string, value *Item) error
	ListDel(key string, value *Item)
	ListGet(key string) []*Item
}

type setReq struct {
	key  string
	item Item
}

type getReq struct {
	key      string
	resp     chan Item
	notFound chan bool
}

type delReq struct {
	key  string
	resp chan bool
}

// Store implements a key/value in-memory storage
type store struct {
	kval  map[string]Item
	ktree map[string]*btree.BTree
	set   chan setReq
	get   chan getReq
	del   chan delReq
}

// NewStore returns a new instance of Store
func NewStore() Store {
	s := &store{
		kval: make(map[string]Item),
	}
	return s
}

// Init initializes internal goroutines
func (s *store) Init() {
	s.set = make(chan setReq)
	s.get = make(chan getReq)
	s.del = make(chan delReq)
	go func() {
		defer func() {
			fmt.Println("Store closed")
		}()
		for {
			select {
			case r, ok := <-s.set:
				if !ok {
					return
				}
				log.Printf("set key: \"%s\" item id: \"%s\"", r.key, r.item.ID)
				s.kval[r.key] = r.item

			case r := <-s.get:
				if val, ok := s.kval[r.key]; ok {
					r.resp <- val
				} else {
					r.notFound <- true
				}

			case r := <-s.del:
				delete(s.kval, r.key)
				r.resp <- true

			}
		}
	}()
}

// Close stops all internal goroutines
func (s *store) Close() {
	if s.set != nil {
		close(s.set)
	}
}

// Set sets a value to a key given an optional duration.
func (s *store) Set(key string, value *Item, d time.Duration) error {
	if s.set == nil {
		return fmt.Errorf("ERROR: Init must be called first")
	}
	if value == nil {
		return fmt.Errorf("ERROR: nil value")
	}
	if len(key) == 0 || len(value.ID) == 0 {
		return fmt.Errorf("invalid input")
	}
	req := &setReq{
		key:  key,
		item: *value,
	}
	select {
	case s.set <- *req:
	case <-time.After(3 * time.Second):
		return fmt.Errorf("ERROR: send timeout")
	}
	return nil
}

// Get returns the item given the key
func (s *store) Get(key string) (item *Item, found bool, err error) {
	req := &getReq{
		key:      key,
		resp:     make(chan Item),
		notFound: make(chan bool),
	}
	select {
	case s.get <- *req:
	case <-time.After(3 * time.Second):
		return nil, false, fmt.Errorf("Get channel timeout")
	}
	select {
	case i := <-req.resp:
		return &i, true, nil

	case <-req.notFound:
		return nil, false, nil
	}
}

// Del deletes the item for the key
func (s *store) Del(key string) error {
	if len(key) == 0 {
		return fmt.Errorf("Invalid key")
	}
	req := &delReq{
		key:  key,
		resp: make(chan bool),
	}
	select {
	case s.del <- *req:
	case <-time.After(3 * time.Second):
		return fmt.Errorf("Del channel timeout")
	}
	<-req.resp
	return nil
}

// ListPush adds the item to the list of items
func (s *store) ListPush(key string, value *Item) error {
	return nil
}

// ListDel deletes the item from the list
func (s *store) ListDel(key string, value *Item) {
}

// ListGet returns the list of items given a key
func (s *store) ListGet(key string) []*Item {
	var items []*Item
	return items
}
