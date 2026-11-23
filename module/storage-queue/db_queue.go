package main

import (
	"bytes"
	"encoding/binary"
	"regexp"
	"strconv"

	"github.com/pantopic/ext-mdb/sdk-go"

	"github.com/pantopic/kitsune/module/pkg/globals"
	"github.com/pantopic/kitsune/module/pkg/pb"
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

func (db dbQueueImpl) create(name []byte) (q queue, err error) {
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

func defaultQueueAttributes() map[string]string {
	return map[string]string{
		"DelaySeconds":                  "0",
		"MaximumMessageSize":            strconv.FormatUint(globals.MAX_MESSAGE_SIZE(), 10),
		"MessageRetentionPeriod":        strconv.FormatUint(globals.DEFAULT_MESSAGE_RETENTION(), 10),
		"ReceiveMessageWaitTimeSeconds": "0",
		"VisibilityTimeout":             strconv.FormatUint(globals.DEFAULT_MESSAGE_VISIBILITY(), 10),
	}
}

func normalizeQueueAttributes(attributes map[string]string) (map[string]string, error) {
	result := defaultQueueAttributes()
	for name, value := range attributes {
		minimum, maximum := uint64(0), uint64(0)
		switch name {
		case "DelaySeconds":
			maximum = globals.MAX_DELAY_TIME()
		case "MaximumMessageSize":
			minimum, maximum = 1<<10, globals.MAX_MESSAGE_SIZE()
		case "MessageRetentionPeriod":
			minimum, maximum = 60, globals.MAX_MESSAGE_RETENTION()
		case "ReceiveMessageWaitTimeSeconds":
			maximum = globals.MAX_POLL_WAIT_TIME()
		case "VisibilityTimeout":
			maximum = globals.MAX_MESSAGE_VISIBILITY()
		default:
			return nil, ErrQueueAttributeNameInvalid
		}
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil || parsed < minimum || parsed > maximum {
			return nil, ErrQueueAttributeValueInvalid
		}
		result[name] = value
	}
	return result, nil
}

func (db dbQueueImpl) createInTxn(txn mdb.Txn, name []byte, attributes map[string]string) (q queue, err error) {
	q, err = db.create(name)
	if err != nil {
		return
	}
	normalized, err := normalizeQueueAttributes(attributes)
	if err != nil {
		return
	}
	if _, err = txn.Get(db.id.i, name, nil); err == nil {
		existing, getErr := db.getAttributes(txn, name)
		if getErr != nil {
			err = getErr
			return
		}
		if !sameQueueAttributes(existing, normalized) {
			err = ErrQueueNameExists
		}
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
	if err = db.putAttributes(txn, name, normalized); err != nil {
		return
	}
	if err = db.stat.putUint64(txn, idKey, 0); err != nil {
		return
	}
	return
}

func sameQueueAttributes(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for name, value := range a {
		if b[name] != value {
			return false
		}
	}
	return true
}

func (db dbQueueImpl) getID(txn mdb.Txn, name []byte) (uint64, error) {
	return db.id.getUint64(txn, name)
}

func (db dbQueueImpl) getAttributes(txn mdb.Txn, name []byte) (map[string]string, error) {
	id, err := db.getID(txn, name)
	if mdb.IsNotFound(err) {
		return nil, ErrQueueDoesNotExist
	}
	if err != nil {
		return nil, err
	}
	idKey := binary.BigEndian.AppendUint64(nil, id)
	value, err := txn.Get(db.attr.i, idKey, nil)
	if err != nil {
		return nil, err
	}
	value, err = db.attr.trimChecksum(idKey, value)
	if err != nil {
		return nil, err
	}
	separator := bytes.IndexByte(value, 0)
	if separator < 0 {
		return defaultQueueAttributes(), nil
	}
	properties := &pb.QueueProperties{}
	if err := properties.UnmarshalVT(value[separator+1:]); err != nil {
		return nil, err
	}
	attributes := defaultQueueAttributes()
	for name, value := range properties.Attributes {
		attributes[name] = value
	}
	return attributes, nil
}

func (db dbQueueImpl) putAttributes(txn mdb.Txn, name []byte, attributes map[string]string) error {
	id, err := db.getID(txn, name)
	if err != nil {
		return err
	}
	properties, err := (&pb.QueueProperties{Attributes: attributes}).MarshalVT()
	if err != nil {
		return err
	}
	idKey := binary.BigEndian.AppendUint64(nil, id)
	value := append(append(append([]byte(nil), name...), 0), properties...)
	return txn.Put(db.attr.i, idKey, db.attr.addChecksum(idKey, value), 0)
}

func (db dbQueueImpl) setAttributes(txn mdb.Txn, name []byte, attributes map[string]string) error {
	current, err := db.getAttributes(txn, name)
	if err != nil {
		return err
	}
	for key, value := range attributes {
		current[key] = value
	}
	normalized, err := normalizeQueueAttributes(current)
	if err != nil {
		return err
	}
	return db.putAttributes(txn, name, normalized)
}

func (db dbQueueImpl) delete(txn mdb.Txn, name []byte) error {
	id, err := db.getID(txn, name)
	if mdb.IsNotFound(err) {
		return ErrQueueDoesNotExist
	}
	if err != nil {
		return err
	}
	idKey := binary.BigEndian.AppendUint64(nil, id)
	if err := dbMessage.purge(txn, id); err != nil {
		return err
	}
	if err = txn.Del(db.id.i, name, nil); err != nil {
		return err
	}
	if err = txn.Del(db.attr.i, idKey, nil); err != nil {
		return err
	}
	return txn.Del(db.stat.i, idKey, nil)
}

func (db dbQueueImpl) purge(txn mdb.Txn, name []byte) error {
	id, err := db.getID(txn, name)
	if mdb.IsNotFound(err) {
		return ErrQueueDoesNotExist
	}
	if err != nil {
		return err
	}
	if err := dbMessage.purge(txn, id); err != nil {
		return err
	}
	return db.stat.putUint64(txn, binary.BigEndian.AppendUint64(nil, id), 0)
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
