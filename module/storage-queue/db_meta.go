package main

import (
	"github.com/pantopic/ext-mdb/sdk-go"
)

var (
	// Index of last applied raft log entry
	metaKeyIndex = []byte(`index`)

	// Shard raft term - Prevents duplicate controllers
	metaKeyTerm = []byte(`term`)

	// Logical clock representing seconds of uptime since shard creation (roughly)
	metaKeyEpoch = []byte(`epoch`)

	// Autoincrement cursor for generating queue ids
	metaKeyQueueID = []byte(`queue_id`)
)

type dbMetaImpl struct {
	db
}

func (db dbMetaImpl) init(txn mdb.Txn) (index uint64) {
	var err error
	db.open(txn)
	for _, k := range [][]byte{
		metaKeyIndex,
		metaKeyTerm,
		metaKeyEpoch,
		metaKeyQueueID,
	} {
		if _, err = db.db.getUint64(txn, k); mdb.IsNotFound(err) {
			err = db.putUint64(txn, k, 0)
		}
		if err != nil {
			panic(err)
		}
	}
	if index, err = db.getIndex(txn); err != nil {
		panic(err)
	}
	return
}

func (db dbMetaImpl) getIndex(txn mdb.Txn) (val uint64, err error) {
	return db.getUint64(txn, metaKeyIndex)
}

func (db dbMetaImpl) setIndex(txn mdb.Txn, val uint64) (err error) {
	return db.putUint64(txn, metaKeyIndex, val)
}

func (db dbMetaImpl) getTerm(txn mdb.Txn) (val uint64, err error) {
	return db.getUint64(txn, metaKeyTerm)
}

func (db dbMetaImpl) setTerm(txn mdb.Txn, val uint64) (err error) {
	return db.putUint64(txn, metaKeyTerm, val)
}

func (db dbMetaImpl) getEpoch(txn mdb.Txn) (val uint64, err error) {
	return db.getUint64(txn, metaKeyEpoch)
}

func (db dbMetaImpl) setEpoch(txn mdb.Txn, val uint64) (err error) {
	return db.putUint64(txn, metaKeyEpoch, val)
}

func (db dbMetaImpl) getQueueID(txn mdb.Txn) (val uint64, err error) {
	return db.getUint64(txn, metaKeyQueueID)
}

func (db dbMetaImpl) setQueueID(txn mdb.Txn, val uint64) (err error) {
	return db.putUint64(txn, metaKeyQueueID, val)
}
