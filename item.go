package gostore

import (
	"time"
)

// Item is the item to be stored
type Item struct {
	ID    string      // the unique item ID
	Key   string      // the key in the key/value store
	Value interface{} // the value in the key/value store

	expiresAt time.Time
}
