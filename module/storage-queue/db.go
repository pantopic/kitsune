package main

import (
	"encoding/binary"
	"strconv"

	"github.com/pantopic/ext-mdb/sdk-go"
)

const (
	DBI_META mdb.DBI = 2 + iota
	DBI_QUEUE_ID
	DBI_QUEUE_ATTR
	DBI_QUEUE_STAT
	DBI_QUEUE_STAT_TS
	DBI_GROUP_ID
	DBI_GROUP_NAME
	DBI_GROUP_STAT
	DBI_MESSAGE_PENDING
	DBI_MESSAGE_ACTIVE
	DBI_MESSAGE_EXPIRE
	DBI_MESSAGE_ATTR
	DBI_MESSAGE_DATA
)

var (
	dbMeta  = dbMetaImpl{db{`meta`, DBI_META, mdb.Create}}
	dbQueue = dbQueueImpl{
		id:    db{`queue_id`, DBI_QUEUE_ID, mdb.Create},
		attr:  db{`queue_attr`, DBI_QUEUE_ATTR, mdb.Create},
		stat:  db{`queue_stat`, DBI_QUEUE_STAT, mdb.Create},
		stats: db{`queue_stat_ts`, DBI_QUEUE_STAT_TS, mdb.Create},
	}
	dbGroup = dbGroupImpl{
		id:   db{`group_id`, DBI_GROUP_ID, mdb.Create},
		name: db{`group_name`, DBI_GROUP_NAME, mdb.Create},
		stat: db{`group_stat`, DBI_GROUP_STAT, mdb.Create},
	}
	dbMessage = dbMessageImpl{
		pending: db{`message_pending`, DBI_MESSAGE_PENDING, mdb.Create | mdb.DupSort},
		active:  db{`message_active`, DBI_MESSAGE_ACTIVE, mdb.Create | mdb.DupSort},
		expire:  db{`message_expire`, DBI_MESSAGE_EXPIRE, mdb.Create | mdb.DupSort},
		attr:    db{`message_attr`, DBI_MESSAGE_ATTR, mdb.Create},
		data:    db{`message_data`, DBI_MESSAGE_DATA, mdb.Create},
	}
)

type db struct {
	name  string
	i     mdb.DBI
	flags uint32
}

func (db db) open(txn mdb.Txn) {
	i, err := txn.OpenDBI(db.name, db.flags)
	if err != nil {
		panic(err)
	}
	if i != db.i {
		panic("Incorrect DBI: " + db.name + "(" + strconv.Itoa(int(i)) + ")")
	}
}

func (db db) trimChecksum(key, val []byte) ([]byte, error) {
	if len(val) < 4 {
		return nil, ErrChecksumInvalid
	}
	chk := binary.BigEndian.Uint32(val[len(val)-4:])
	val = val[:len(val)-4]
	if chk != crc(key, val) {
		return nil, ErrChecksumInvalid
	}
	return val, nil
}

func (db db) addChecksum(key, val []byte) []byte {
	return binary.BigEndian.AppendUint32(val, crc(key, val))
}

func (db db) getUint64(txn mdb.Txn, key []byte) (i uint64, err error) {
	var val []byte
	val, err = txn.Get(db.i, key, val)
	if err != nil {
		return
	}
	val, err = db.trimChecksum(key, val)
	if err != nil {
		return
	}
	if len(val) < 8 {
		err = ErrValueInvalid
	}
	i = binary.BigEndian.Uint64(val[:8])
	return
}

func (db db) putUint64(txn mdb.Txn, key []byte, val uint64) (err error) {
	return txn.Put(db.i, key, db.addChecksum(key, binary.BigEndian.AppendUint64(nil, val)), 0)
}
