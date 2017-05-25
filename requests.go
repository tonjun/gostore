package gostore

type setReq struct {
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
