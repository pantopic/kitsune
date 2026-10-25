package main

import (
	"github.com/pantopic/ext-mdb/sdk-go"
)

type message struct {
	queue queue
	group group
	name  []byte
}

type dbMessageImpl struct {
	pending db
	active  db
	expire  db
	attr    db
	data    db
}

func (db dbMessageImpl) init(txn mdb.Txn) {
	db.pending.open(txn)
	db.active.open(txn)
	db.expire.open(txn)
	db.attr.open(txn)
	db.data.open(txn)
}

func (db dbMessageImpl) create(
	txn mdb.Txn,
	q queue,
	g group,
	data []byte,
) (m message, err error) {
	m = message{queue: q, group: g, name: data}
	return
}
