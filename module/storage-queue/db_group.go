package main

import (
	"github.com/pantopic/ext-mdb/sdk-go"
)

type group struct {
	name []byte
}

type dbGroupImpl struct {
	id   db
	name db
	stat db
}

func (db dbGroupImpl) init(txn mdb.Txn) {
	db.id.open(txn)
	db.name.open(txn)
	db.stat.open(txn)
}

func (db dbGroupImpl) create(
	txn mdb.Txn,
	name []byte,
) (g group, err error) {
	if len(name) == 0 {
		err = ErrGroupNameRequired
		return
	}
	if len(name) > int(MAX_MESSAGE_GROUP_ID_LEN()) {
		err = ErrGroupNameTooLong
		return
	}
	g = group{name: name}
	return
}
