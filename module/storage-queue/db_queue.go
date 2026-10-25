package main

import (
	"github.com/pantopic/ext-mdb/sdk-go"
)

type queue struct {
	name []byte
}

type dbQueueImpl struct {
	id    db
	attr  db
	stat  db
	stats db
}

func (db dbQueueImpl) init(txn mdb.Txn) {
	db.id.open(txn)
	db.attr.open(txn)
	db.stat.open(txn)
	db.stats.open(txn)
}

func (db dbQueueImpl) create(
	txn mdb.Txn,
	name []byte,
) (q queue, err error) {
	if len(name) == 0 {
		err = ErrQueueNameRequired
		return
	}
	if len(name) > int(MAX_QUEUE_NAME_LEN()) {
		err = ErrQueueNameTooLong
		return
	}
	q = queue{name: name}
	return
}
