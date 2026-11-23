package main

import (
	"bytes"
	"encoding/binary"
	"regexp"

	"github.com/pantopic/ext-mdb/sdk-go"

	"github.com/pantopic/kitsune/module/pkg/globals"
)

var queueNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

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
	if len(name) > int(globals.MAX_QUEUE_NAME_LEN()) {
		err = ErrQueueNameTooLong
		return
	}
	if !queueNamePattern.Match(name) {
		err = ErrQueueNameInvalid
		return
	}
	q = queue{name: name}
	return
}

func (db dbQueueImpl) createInTxn(txn mdb.Txn, name []byte) (q queue, err error) {
	q, err = db.create(txn, name)
	if err != nil {
		return
	}
	if _, err = txn.Get(db.id.i, name, nil); err == nil {
		return
	} else if !mdb.IsNotFound(err) {
		return
	}

	id, err := dbMeta.getQueueID(txn)
	if err != nil {
		return
	}
	id++
	if err = dbMeta.setQueueID(txn, id); err != nil {
		return
	}
	idKey := binary.BigEndian.AppendUint64(nil, id)
	if err = txn.Put(db.id.i, name, db.id.addChecksum(name, idKey), 0); err != nil {
		return
	}
	if err = txn.Put(db.attr.i, idKey, db.attr.addChecksum(idKey, name), 0); err != nil {
		return
	}
	if err = db.stat.putUint64(txn, idKey, 0); err != nil {
		return
	}
	return
}

func (db dbQueueImpl) list(txn mdb.Txn, prefix []byte, maxResults uint64) (names [][]byte, err error) {
	cur, err := txn.OpenCursor(db.id.i)
	if err != nil {
		return
	}
	defer cur.Close()

	key, value, err := cur.Get(append([]byte(nil), prefix...), nil, mdb.SetRange)
	for !mdb.IsNotFound(err) {
		if err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(key, prefix) || (maxResults > 0 && uint64(len(names)) >= maxResults) {
			break
		}
		names = append(names, append([]byte(nil), key...))
		key, value, err = cur.Get(key, value, mdb.NextNoDup)
	}
	if err != nil && !mdb.IsNotFound(err) {
		return nil, err
	}
	return names, nil
}
