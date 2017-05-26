package gostore

import (
	"fmt"
	"log"
	"time"

	"github.com/google/btree"
)

type kvStore struct {
	kval         map[string]Item
	set          chan setReq
	get          chan getReq
	del          chan delReq
	forExpiry    *btree.BTree // list of items to be checked for expiry
	itemExpireCb func(*Item)
}

func newKVStore() *kvStore {
	return &kvStore{
		kval:      make(map[string]Item),
		forExpiry: btree.New(32),
	}
}

func (s *kvStore) init() {
	s.set = make(chan setReq)
	s.get = make(chan getReq)
	s.del = make(chan delReq)

	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)

		defer func() {
			//log.Println("kvStore closed")
			ticker.Stop()
		}()

		for {
			select {
			case r, ok := <-s.set:
				if !ok {
					return
				}
				s.kval[r.item.Key] = r.item
				if !r.item.expiresAt.IsZero() {

					// add to forExpiry tree
					ti := treeItem{
						Key:   r.item.Key,
						Value: &r.item,
					}
					s.forExpiry.ReplaceOrInsert(ti)

				}

			case r := <-s.get:
				if val, ok := s.kval[r.key]; ok {
					r.resp <- val
				} else {
					r.notFound <- true
				}

			case r := <-s.del:
				s.deleteItem(r.key)
				r.resp <- true

			case <-ticker.C:
				s.checkExpiredItems()

			}
		}
	}()
}

func (s *kvStore) closeStore() {
	if s.set != nil {
		close(s.set)
	}
}

func (s *kvStore) put(item *Item, d time.Duration) error {
	if s.set == nil {
		log.Printf("ERROR: Init must be called first")
		return fmt.Errorf("ERROR: Init must be called first")
	}
	if item == nil {
		return fmt.Errorf("ERROR: nil item")
	}
	if len(item.Key) == 0 || len(item.ID) == 0 {
		return fmt.Errorf("invalid item")
	}
	if d > 0 {
		item.expiresAt = time.Now().Add(d)
	}
	req := &setReq{
		item: *item,
	}
	select {
	case s.set <- *req:
	case <-time.After(3 * time.Second):
		return fmt.Errorf("ERROR: send timeout")
	}
	return nil
}

func (s *kvStore) getItem(key string) (item *Item, found bool, err error) {
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

func (s *kvStore) delItem(key string) error {
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

func (s *kvStore) checkExpiredItems() {
	n := time.Now()
	s.forExpiry.Ascend(func(a btree.Item) bool {
		i := a.(treeItem).Value
		d := n.Unix() - i.expiresAt.Unix()
		key := i.Key
		if d >= 0 {
			//log.Printf("item: key: %s expired. diff: %d", key, d)
			if s.itemExpireCb != nil {
				go func(k string, v Item) {
					// trigger the OnItemDidExpire callback
					s.itemExpireCb(&v)
				}(key, *i)
			}
			go s.delItem(key)
		} else {
			//log.Printf("item: key: %s not yet expired. diff: %d", key, d)
		}
		return true
	})
}

func (s *kvStore) deleteItem(key string) {
	if val, ok := s.kval[key]; ok {
		if !val.expiresAt.IsZero() {
			ti := treeItem{
				Key:   val.Key,
				Value: &val,
			}
			s.forExpiry.Delete(ti)
		}
	}
	delete(s.kval, key)
}

func (s *kvStore) onItemDidExpire(cb func(item *Item)) {
	s.itemExpireCb = cb
}
