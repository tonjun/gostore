// Package gostore is an in-memory key/value store
package gostore

import (
	"time"
)

// Store implements a key/value in-memory storage
type Store struct {
}

// NewStore returns a new instance of Store
func NewStore() *Store {
	s := &Store{}
	return s
}

// Init initializes internal goroutines
func (s *Store) Init() {
}

// Close stops all internal goroutines
func (s *Store) Close() {
}

// Set sets a value to a key given an optional duration.
func (s *Store) Set(key string, value *Item, d time.Duration) error {
	return nil
}

// Get returns the item given the key
func (s *Store) Get(key string) (item *Item, found bool) {
	return item, found
}

// Del deletes the item for the key
func (s *Store) Del(key string) {
}

// ListPush adds the item to the list of items
func (s *Store) ListPush(key string, value *Item) error {
	return nil
}

// ListDel deletes the item from the list
func (s *Store) ListDel(key string, value *Item) {
}

// ListGet returns the list of items given a key
func (s *Store) ListGet(key string) []*Item {
	var items []*Item
	return items
}
