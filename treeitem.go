package gostore

import (
	"github.com/google/btree"
)

type treeItem struct {
	Key   string
	Value *Item
}

func (a treeItem) Less(b btree.Item) bool {
	return a.Key < b.(treeItem).Key
}
