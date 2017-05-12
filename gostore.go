// Package gostore is an in-memory key/value store
package gostore

import (
	"fmt"
	"log"
	"time"

	"github.com/google/btree"
)

// Store is the interface to the in-memory store
type Store interface {

	// Init initializes the store
	Init()

	// Close stops all internal goroutines
	Close()

	// Set sets a value to a key given an optional duration.
	Set(key string, value *Item, d time.Duration) error

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

	// OnItemExpire adds the callback function to the list off callback functions
	// called when an item expires
	OnItemExpire(func(key string, item *Item))
}

// NewStore returns a new instance of Store
func NewStore() Store {
	s := &store{
		kval:  make(map[string]Item),
		ktree: make(map[string]*btree.BTree),
	}
	return s
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

type listPushReq struct {
	key  string
	item Item
}

type listGetReq struct {
	key      string
	resp     chan []*Item
	notFound chan bool
}

type listDelReq struct {
	key  string
	item Item
	resp chan bool
}

type treeItem struct {
	Key   string
	Value *Item
}

func (a treeItem) Less(b btree.Item) bool {
	return a.Key < b.(treeItem).Key
}

// Store implements a key/value in-memory storage
type store struct {
	kval  map[string]Item
	ktree map[string]*btree.BTree
	set   chan setReq
	get   chan getReq
	del   chan delReq
	lpush chan listPushReq
	lget  chan listGetReq
	ldel  chan listDelReq

	itemExpireCb func(string, *Item)
}

func (s *store) Init() {
	s.set = make(chan setReq)
	s.get = make(chan getReq)
	s.del = make(chan delReq)
	s.lpush = make(chan listPushReq)
	s.lget = make(chan listGetReq)
	s.ldel = make(chan listDelReq)
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
				//log.Printf("set key: \"%s\" item id: \"%s\"", r.key, r.item.ID)
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

			case r := <-s.lpush:
				ti := treeItem{
					Key:   r.item.ID,
					Value: &r.item,
				}
				s.getTree(r.key).ReplaceOrInsert(ti)

			case r := <-s.lget:
				if _, ok := s.ktree[r.key]; !ok {
					r.notFound <- true
				} else {
					items := make([]*Item, 0)
					s.getTree(r.key).Ascend(func(a btree.Item) bool {
						items = append(items, a.(treeItem).Value)
						return true
					})
					r.resp <- items
				}

			case r := <-s.ldel:
				ti := treeItem{
					Key:   r.item.ID,
					Value: &r.item,
				}
				s.getTree(r.key).Delete(ti)
				r.resp <- true

			}
		}
	}()
}

func (s *store) Close() {
	if s.set != nil {
		close(s.set)
	}
}

func (s *store) Set(key string, value *Item, d time.Duration) error {
	if s.set == nil {
		log.Printf("ERROR: Init must be called first")
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

func (s *store) ListPush(key string, value *Item) error {
	if s.set == nil {
		log.Printf("ERROR: Init must be called first")
		return fmt.Errorf("ERROR: Init must be called first")
	}
	if value == nil {
		return fmt.Errorf("ERROR: nil value")
	}
	if len(key) == 0 || len(value.ID) == 0 {
		return fmt.Errorf("invalid input")
	}
	req := listPushReq{
		key:  key,
		item: *value,
	}
	select {
	case s.lpush <- req:
	case <-time.After(3 * time.Second):
		return fmt.Errorf("ERROR: channel timeout")
	}
	return nil
}

func (s *store) ListDel(key string, value *Item) error {
	if s.set == nil {
		log.Printf("ERROR: Init must be called first")
		return fmt.Errorf("ERROR: Init must be called first")
	}
	if value == nil {
		return fmt.Errorf("ERROR: nil value")
	}
	if len(key) == 0 || len(value.ID) == 0 {
		return fmt.Errorf("invalid input")
	}
	req := listDelReq{
		key:  key,
		item: *value,
		resp: make(chan bool),
	}
	select {
	case s.ldel <- req:
	case <-time.After(3 * time.Second):
		return fmt.Errorf("Del channel timeout")
	}
	<-req.resp
	return nil
}

func (s *store) ListGet(key string) ([]*Item, bool, error) {
	var items []*Item

	req := listGetReq{
		key:      key,
		resp:     make(chan []*Item),
		notFound: make(chan bool),
	}
	select {
	case s.lget <- req:
	case <-time.After(3 * time.Second):
		return nil, false, fmt.Errorf("Get channel timeout")
	}
	select {
	case items = <-req.resp:
		return items, true, nil
	case <-req.notFound:
		return make([]*Item, 0), false, nil
	}
}

func (s *store) getTree(key string) *btree.BTree {
	var tree *btree.BTree
	if t, ok := s.ktree[key]; !ok {
		tree = btree.New(32)
		s.ktree[key] = tree
	} else {
		tree = t
	}
	return tree
}

func (s *store) OnItemExpire(cb func(key string, item *Item)) {
	s.itemExpireCb = cb
}
