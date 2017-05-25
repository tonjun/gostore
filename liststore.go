package gostore

import (
	"fmt"
	"log"
	"time"

	"github.com/google/btree"
)

type listStore struct {
	lpush        chan listPushReq
	lget         chan listGetReq
	ldel         chan listDelReq
	close        chan bool
	ktree        map[string]*btree.BTree
	listChangeCb func(string, []*Item)
}

func newListStore() *listStore {
	return &listStore{
		close: make(chan bool),
		ktree: make(map[string]*btree.BTree),
	}
}

func (s *listStore) init() {
	s.lpush = make(chan listPushReq)
	s.lget = make(chan listGetReq)
	s.ldel = make(chan listDelReq)
	go func() {
		defer func() {
			log.Printf("listStore closed")
		}()

		for {
			select {

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

			case <-s.close:
				return

			}
		}
	}()
}

func (s *listStore) closeStore() {
	s.close <- true
}

func (s *listStore) listPush(key string, value *Item) error {
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

func (s *listStore) listDel(key string, value *Item) error {
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

func (s *listStore) listGet(key string) ([]*Item, bool, error) {
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

func (s *listStore) getTree(key string) *btree.BTree {
	var tree *btree.BTree
	if t, ok := s.ktree[key]; !ok {
		tree = btree.New(32)
		s.ktree[key] = tree
	} else {
		tree = t
	}
	return tree
}

func (s *listStore) onListDidChange(cb func(string, []*Item)) {
	s.listChangeCb = cb
}
