package main

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strconv"
	"unicode/utf8"

	"github.com/pantopic/ext-mdb/sdk-go"

	"github.com/pantopic/kitsune/module/pkg/globals"
	"github.com/pantopic/kitsune/module/pkg/pb"
)

var (
	ErrMessageBodyInvalid   = errors.New(`Message body invalid`)
	ErrMessageBodyTooLarge  = errors.New(`Message body too large`)
	ErrMessageDelayInvalid  = errors.New(`Message delay invalid`)
	ErrReceiptHandleInvalid = errors.New(`Receipt handle is invalid`)
	ErrMessageBatchLimit    = errors.New(`Too many messages requested`)
)

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

func (db dbMessageImpl) send(txn mdb.Txn, req *pb.SendMessageRequest, epoch uint64) (*pb.SendMessageResponse, error) {
	if !utf8.ValidString(req.MessageBody) {
		return nil, ErrMessageBodyInvalid
	}
	if uint64(len(req.MessageBody)) > globals.MAX_MESSAGE_SIZE() {
		return nil, ErrMessageBodyTooLarge
	}
	queueID, err := dbQueue.getID(txn, []byte(req.QueueName))
	if mdb.IsNotFound(err) {
		return nil, ErrQueueDoesNotExist
	}
	if err != nil {
		return nil, err
	}
	attributes, err := dbQueue.getAttributes(txn, []byte(req.QueueName))
	if err != nil {
		return nil, err
	}
	maxMessageSize, err := strconv.ParseUint(attributes["MaximumMessageSize"], 10, 64)
	if err != nil {
		return nil, err
	}
	if uint64(len(req.MessageBody)) > maxMessageSize {
		return nil, ErrMessageBodyTooLarge
	}
	delay := req.DelaySeconds
	if delay == 0 {
		delay, _ = strconv.ParseUint(attributes["DelaySeconds"], 10, 64)
	}
	if delay > globals.MAX_DELAY_TIME() {
		return nil, ErrMessageDelayInvalid
	}
	retention, err := strconv.ParseUint(attributes["MessageRetentionPeriod"], 10, 64)
	if err != nil {
		return nil, err
	}

	messageID, err := dbMeta.getMessageID(txn)
	if err != nil {
		return nil, err
	}
	messageID++
	if err := dbMeta.setMessageID(txn, messageID); err != nil {
		return nil, err
	}

	queueIDBytes := binary.BigEndian.AppendUint64(nil, queueID)
	groupIDBytes := make([]byte, 8)
	messageIDBytes := binary.BigEndian.AppendUint64(nil, messageID)
	primaryKey := append(append(append([]byte(nil), queueIDBytes...), groupIDBytes...), messageIDBytes...)
	expires := epoch + retention
	state := &pb.StoredMessage{Expires: expires, SentTimestamp: epoch, ReadyAt: epoch + delay}
	stateBytes, err := state.MarshalVT()
	if err != nil {
		return nil, err
	}
	if err := txn.Put(db.attr.i, primaryKey, db.attr.addChecksum(primaryKey, stateBytes), 0); err != nil {
		return nil, err
	}
	body := []byte(req.MessageBody)
	if err := txn.Put(db.data.i, primaryKey, db.data.addChecksum(primaryKey, body), 0); err != nil {
		return nil, err
	}
	pendingKey := append(append(append([]byte(nil), queueIDBytes...), binary.BigEndian.AppendUint64(nil, epoch+delay)...), groupIDBytes...)
	if err := txn.Put(db.pending.i, pendingKey, messageIDBytes, 0); err != nil {
		return nil, err
	}
	expireKey := append(append(append([]byte(nil), binary.BigEndian.AppendUint64(nil, expires)...), queueIDBytes...), groupIDBytes...)
	if err := txn.Put(db.expire.i, expireKey, messageIDBytes, 0); err != nil {
		return nil, err
	}
	count, err := dbQueue.stat.getUint64(txn, queueIDBytes)
	if err != nil {
		return nil, err
	}
	if err := dbQueue.stat.putUint64(txn, queueIDBytes, count+1); err != nil {
		return nil, err
	}
	digest := md5.Sum(body)
	return &pb.SendMessageResponse{
		MessageId:        hex.EncodeToString(messageIDBytes),
		Md5OfMessageBody: hex.EncodeToString(digest[:]),
	}, nil
}

func (db dbMessageImpl) receive(txn mdb.Txn, req *pb.ReceiveMessageRequest, epoch uint64) (*pb.ReceiveMessageResponse, error) {
	queueID, err := dbQueue.getID(txn, []byte(req.QueueName))
	if mdb.IsNotFound(err) {
		return nil, ErrQueueDoesNotExist
	}
	if err != nil {
		return nil, err
	}
	queueAttributes, err := dbQueue.getAttributes(txn, []byte(req.QueueName))
	if err != nil {
		return nil, err
	}
	visibility := req.VisibilityTimeout
	if visibility == 0 && !req.VisibilityTimeoutSet {
		visibility, err = strconv.ParseUint(queueAttributes["VisibilityTimeout"], 10, 64)
		if err != nil {
			return nil, err
		}
	}
	if visibility > globals.MAX_MESSAGE_VISIBILITY() {
		return nil, ErrQueueAttributeValueInvalid
	}
	maxMessages := req.MaxNumberOfMessages
	if maxMessages == 0 {
		maxMessages = 1
	}
	if maxMessages > globals.MAX_MESSAGE_BATCH_SIZE() {
		return nil, ErrMessageBatchLimit
	}

	queueIDBytes := binary.BigEndian.AppendUint64(nil, queueID)
	startKey := append(append([]byte(nil), queueIDBytes...), make([]byte, 8)...)
	cur, err := txn.OpenCursor(db.pending.i)
	if err != nil {
		return nil, err
	}
	var pendingKeys, messageIDs [][]byte
	key, value, err := cur.Get(startKey, nil, mdb.SetRange)
	for err == nil && uint64(len(messageIDs)) < maxMessages {
		if len(key) < 24 || binary.BigEndian.Uint64(key[:8]) != queueID || binary.BigEndian.Uint64(key[8:16]) > epoch {
			break
		}
		pendingKeys = append(pendingKeys, append([]byte(nil), key...))
		messageIDs = append(messageIDs, append([]byte(nil), value...))
		key, value, err = cur.Get(key, value, mdb.Next)
	}
	cur.Close()
	if err != nil && !mdb.IsNotFound(err) {
		return nil, err
	}

	resp := &pb.ReceiveMessageResponse{}
	for i, pendingKey := range pendingKeys {
		messageIDBytes := messageIDs[i]
		if len(messageIDBytes) != 8 {
			return nil, ErrValueInvalid
		}
		groupIDBytes := pendingKey[16:24]
		primaryKey := append(append(append([]byte(nil), queueIDBytes...), groupIDBytes...), messageIDBytes...)
		stateBytes, err := txn.Get(db.attr.i, primaryKey, nil)
		if err != nil {
			return nil, err
		}
		stateBytes, err = db.attr.trimChecksum(primaryKey, stateBytes)
		if err != nil {
			return nil, err
		}
		state := &pb.StoredMessage{}
		if err := state.UnmarshalVT(stateBytes); err != nil {
			return nil, err
		}
		body, err := txn.Get(db.data.i, primaryKey, nil)
		if err != nil {
			return nil, err
		}
		body, err = db.data.trimChecksum(primaryKey, body)
		if err != nil {
			return nil, err
		}
		if err := txn.Del(db.pending.i, pendingKey, messageIDBytes); err != nil {
			return nil, err
		}

		if state.ActiveUntil > 0 {
			previousActiveKey := append(append(append([]byte(nil), binary.BigEndian.AppendUint64(nil, state.ActiveUntil)...), queueIDBytes...), groupIDBytes...)
			if err := txn.Del(db.active.i, previousActiveKey, messageIDBytes); err != nil && !mdb.IsNotFound(err) {
				return nil, err
			}
		}

		timeout := epoch + visibility
		activeKey := append(append(append([]byte(nil), binary.BigEndian.AppendUint64(nil, timeout)...), queueIDBytes...), groupIDBytes...)
		if err := txn.Put(db.active.i, activeKey, messageIDBytes, 0); err != nil {
			return nil, err
		}
		state.ReceiveCount++
		state.ActiveUntil = timeout
		if visibility == 0 {
			state.ReadyAt = epoch
			readyKey := append(append(append([]byte(nil), queueIDBytes...), binary.BigEndian.AppendUint64(nil, epoch)...), groupIDBytes...)
			if err := txn.Put(db.pending.i, readyKey, messageIDBytes, 0); err != nil {
				return nil, err
			}
		}
		stateBytes, err = state.MarshalVT()
		if err != nil {
			return nil, err
		}
		if err := txn.Put(db.attr.i, primaryKey, db.attr.addChecksum(primaryKey, stateBytes), 0); err != nil {
			return nil, err
		}
		receipt := append(append(append(append([]byte(nil), queueIDBytes...), groupIDBytes...), messageIDBytes...), binary.BigEndian.AppendUint64(nil, timeout)...)
		resp.Messages = append(resp.Messages, &pb.ReceivedMessage{
			MessageId:     hex.EncodeToString(messageIDBytes),
			ReceiptHandle: hex.EncodeToString(receipt),
			Body:          string(body),
		})
	}
	return resp, nil
}

func (db dbMessageImpl) tick(txn mdb.Txn, epoch uint64) error {
	expiredKeys, expiredIDs, err := db.due(txn, db.expire, epoch)
	if err != nil {
		return err
	}
	for i, expireKey := range expiredKeys {
		messageID := expiredIDs[i]
		if len(expireKey) != 24 || len(messageID) != 8 {
			return ErrKeyInvalid
		}
		primaryKey := append(append([]byte(nil), expireKey[8:24]...), messageID...)
		stateBytes, err := txn.Get(db.attr.i, primaryKey, nil)
		if mdb.IsNotFound(err) {
			if err := txn.Del(db.expire.i, expireKey, messageID); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		stateBytes, err = db.attr.trimChecksum(primaryKey, stateBytes)
		if err != nil {
			return err
		}
		state := &pb.StoredMessage{}
		if err := state.UnmarshalVT(stateBytes); err != nil {
			return err
		}
		queueIDBytes := expireKey[8:16]
		groupIDBytes := expireKey[16:24]
		if state.ActiveUntil > 0 {
			activeKey := append(append(append([]byte(nil), binary.BigEndian.AppendUint64(nil, state.ActiveUntil)...), queueIDBytes...), groupIDBytes...)
			if err := txn.Del(db.active.i, activeKey, messageID); err != nil && !mdb.IsNotFound(err) {
				return err
			}
		}
		pendingKey := append(append(append([]byte(nil), queueIDBytes...), binary.BigEndian.AppendUint64(nil, state.ReadyAt)...), groupIDBytes...)
		if err := txn.Del(db.pending.i, pendingKey, messageID); err != nil && !mdb.IsNotFound(err) {
			return err
		}
		if err := txn.Del(db.expire.i, expireKey, messageID); err != nil {
			return err
		}
		if err := txn.Del(db.attr.i, primaryKey, nil); err != nil {
			return err
		}
		if err := txn.Del(db.data.i, primaryKey, nil); err != nil {
			return err
		}
		count, err := dbQueue.stat.getUint64(txn, queueIDBytes)
		if err != nil {
			return err
		}
		if count > 0 {
			if err := dbQueue.stat.putUint64(txn, queueIDBytes, count-1); err != nil {
				return err
			}
		}
	}

	activeKeys, activeIDs, err := db.due(txn, db.active, epoch)
	if err != nil {
		return err
	}
	for i, activeKey := range activeKeys {
		messageID := activeIDs[i]
		if len(activeKey) != 24 || len(messageID) != 8 {
			return ErrKeyInvalid
		}
		queueIDBytes := activeKey[8:16]
		groupIDBytes := activeKey[16:24]
		primaryKey := append(append([]byte(nil), queueIDBytes...), groupIDBytes...)
		primaryKey = append(primaryKey, messageID...)
		stateBytes, err := txn.Get(db.attr.i, primaryKey, nil)
		if mdb.IsNotFound(err) {
			if err := txn.Del(db.active.i, activeKey, messageID); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
		stateBytes, err = db.attr.trimChecksum(primaryKey, stateBytes)
		if err != nil {
			return err
		}
		state := &pb.StoredMessage{}
		if err := state.UnmarshalVT(stateBytes); err != nil {
			return err
		}
		if state.Expires <= epoch {
			continue
		}
		if err := txn.Del(db.active.i, activeKey, messageID); err != nil {
			return err
		}
		state.ActiveUntil = 0
		state.ReadyAt = epoch
		stateBytes, err = state.MarshalVT()
		if err != nil {
			return err
		}
		if err := txn.Put(db.attr.i, primaryKey, db.attr.addChecksum(primaryKey, stateBytes), 0); err != nil {
			return err
		}
		pendingKey := append(append(append([]byte(nil), queueIDBytes...), binary.BigEndian.AppendUint64(nil, epoch)...), groupIDBytes...)
		if err := txn.Put(db.pending.i, pendingKey, messageID, 0); err != nil {
			return err
		}
	}
	return nil
}

func (db dbMessageImpl) due(txn mdb.Txn, index db, epoch uint64) (keys, values [][]byte, err error) {
	cur, err := txn.OpenCursor(index.i)
	if err != nil {
		return nil, nil, err
	}
	defer cur.Close()
	key, value, err := cur.Get(make([]byte, 8), nil, mdb.SetRange)
	for err == nil {
		if len(key) != 24 || binary.BigEndian.Uint64(key[:8]) > epoch {
			break
		}
		keys = append(keys, append([]byte(nil), key...))
		values = append(values, append([]byte(nil), value...))
		key, value, err = cur.Get(key, value, mdb.Next)
	}
	if err != nil && !mdb.IsNotFound(err) {
		return nil, nil, err
	}
	return keys, values, nil
}

func (db dbMessageImpl) delete(txn mdb.Txn, req *pb.DeleteMessageRequest) error {
	receipt, err := hex.DecodeString(req.ReceiptHandle)
	if err != nil || len(receipt) != 32 {
		return ErrReceiptHandleInvalid
	}
	queueID, err := dbQueue.getID(txn, []byte(req.QueueName))
	if mdb.IsNotFound(err) || (err == nil && queueID != binary.BigEndian.Uint64(receipt[:8])) {
		return ErrReceiptHandleInvalid
	}
	if err != nil {
		return err
	}
	messageIDBytes := receipt[16:24]
	timeout := binary.BigEndian.Uint64(receipt[24:32])
	activeKey := append(binary.BigEndian.AppendUint64(nil, timeout), receipt[:16]...)
	cur, err := txn.OpenCursor(db.active.i)
	if err != nil {
		return err
	}
	_, _, err = cur.Get(activeKey, messageIDBytes, mdb.GetBoth)
	cur.Close()
	if mdb.IsNotFound(err) {
		return ErrReceiptHandleInvalid
	}
	if err != nil {
		return err
	}
	if err := txn.Del(db.active.i, activeKey, messageIDBytes); err != nil {
		return err
	}
	primaryKey := append([]byte(nil), receipt[:24]...)
	stateBytes, err := txn.Get(db.attr.i, primaryKey, nil)
	if err != nil {
		return err
	}
	stateBytes, err = db.attr.trimChecksum(primaryKey, stateBytes)
	if err != nil {
		return err
	}
	state := &pb.StoredMessage{}
	if err := state.UnmarshalVT(stateBytes); err != nil {
		return err
	}
	pendingKey := append(append(append([]byte(nil), receipt[:8]...), binary.BigEndian.AppendUint64(nil, state.ReadyAt)...), receipt[8:16]...)
	if err := txn.Del(db.pending.i, pendingKey, messageIDBytes); err != nil && !mdb.IsNotFound(err) {
		return err
	}
	expireKey := append(binary.BigEndian.AppendUint64(nil, state.Expires), receipt[:16]...)
	if err := txn.Del(db.expire.i, expireKey, messageIDBytes); err != nil {
		return err
	}
	if err := txn.Del(db.attr.i, primaryKey, nil); err != nil {
		return err
	}
	if err := txn.Del(db.data.i, primaryKey, nil); err != nil {
		return err
	}
	queueIDBytes := receipt[:8]
	count, err := dbQueue.stat.getUint64(txn, queueIDBytes)
	if err != nil {
		return err
	}
	if count > 0 {
		return dbQueue.stat.putUint64(txn, queueIDBytes, count-1)
	}
	return nil
}

func (db dbMessageImpl) refresh(txn mdb.Txn, req *pb.ChangeMessageVisibilityRequest, epoch uint64) error {
	if req.VisibilityTimeout > globals.MAX_MESSAGE_VISIBILITY() {
		return ErrQueueAttributeValueInvalid
	}
	receipt, err := hex.DecodeString(req.ReceiptHandle)
	if err != nil || len(receipt) != 32 {
		return ErrReceiptHandleInvalid
	}
	queueID, err := dbQueue.getID(txn, []byte(req.QueueName))
	if mdb.IsNotFound(err) || (err == nil && queueID != binary.BigEndian.Uint64(receipt[:8])) {
		return ErrReceiptHandleInvalid
	}
	if err != nil {
		return err
	}
	messageIDBytes := receipt[16:24]
	activeKey := append(binary.BigEndian.AppendUint64(nil, binary.BigEndian.Uint64(receipt[24:32])), receipt[:16]...)
	cur, err := txn.OpenCursor(db.active.i)
	if err != nil {
		return err
	}
	_, _, err = cur.Get(activeKey, messageIDBytes, mdb.GetBoth)
	cur.Close()
	if mdb.IsNotFound(err) {
		return ErrReceiptHandleInvalid
	}
	if err != nil {
		return err
	}
	primaryKey := append([]byte(nil), receipt[:24]...)
	stateBytes, err := txn.Get(db.attr.i, primaryKey, nil)
	if err != nil {
		return err
	}
	stateBytes, err = db.attr.trimChecksum(primaryKey, stateBytes)
	if err != nil {
		return err
	}
	state := &pb.StoredMessage{}
	if err := state.UnmarshalVT(stateBytes); err != nil {
		return err
	}
	if err := txn.Del(db.active.i, activeKey, messageIDBytes); err != nil {
		return err
	}
	pendingKey := append(append(append([]byte(nil), receipt[:8]...), binary.BigEndian.AppendUint64(nil, state.ReadyAt)...), receipt[8:16]...)
	if err := txn.Del(db.pending.i, pendingKey, messageIDBytes); err != nil && !mdb.IsNotFound(err) {
		return err
	}
	state.ActiveUntil = epoch + req.VisibilityTimeout
	if req.VisibilityTimeout == 0 {
		state.ActiveUntil = 0
		state.ReadyAt = epoch
		queueIDBytes := receipt[:8]
		groupIDBytes := receipt[8:16]
		pendingKey := append(append(append([]byte(nil), queueIDBytes...), binary.BigEndian.AppendUint64(nil, epoch)...), groupIDBytes...)
		if err := txn.Put(db.pending.i, pendingKey, messageIDBytes, 0); err != nil {
			return err
		}
	} else {
		newActiveKey := append(binary.BigEndian.AppendUint64(nil, state.ActiveUntil), receipt[:16]...)
		if err := txn.Put(db.active.i, newActiveKey, messageIDBytes, 0); err != nil {
			return err
		}
	}
	stateBytes, err = state.MarshalVT()
	if err != nil {
		return err
	}
	return txn.Put(db.attr.i, primaryKey, db.attr.addChecksum(primaryKey, stateBytes), 0)
}

func (db dbMessageImpl) purge(txn mdb.Txn, queueID uint64) error {
	queueIDBytes := binary.BigEndian.AppendUint64(nil, queueID)
	cur, err := txn.OpenCursor(db.attr.i)
	if err != nil {
		return err
	}
	key, value, err := cur.Get(queueIDBytes, nil, mdb.SetRange)
	var primaryKeys [][]byte
	for err == nil && len(key) >= 8 && binary.BigEndian.Uint64(key[:8]) == queueID {
		primaryKeys = append(primaryKeys, append([]byte(nil), key...))
		key, value, err = cur.Get(key, value, mdb.NextNoDup)
	}
	cur.Close()
	if err != nil && !mdb.IsNotFound(err) {
		return err
	}
	for _, primaryKey := range primaryKeys {
		if len(primaryKey) != 24 {
			return ErrKeyInvalid
		}
		stateBytes, err := txn.Get(db.attr.i, primaryKey, nil)
		if err != nil {
			return err
		}
		stateBytes, err = db.attr.trimChecksum(primaryKey, stateBytes)
		if err != nil {
			return err
		}
		state := &pb.StoredMessage{}
		if err := state.UnmarshalVT(stateBytes); err != nil {
			return err
		}
		groupIDBytes := primaryKey[8:16]
		messageIDBytes := primaryKey[16:24]
		pendingKey := append(append(append([]byte(nil), queueIDBytes...), binary.BigEndian.AppendUint64(nil, state.ReadyAt)...), groupIDBytes...)
		activeKey := append(append(append([]byte(nil), binary.BigEndian.AppendUint64(nil, state.ActiveUntil)...), queueIDBytes...), groupIDBytes...)
		expireKey := append(append(append([]byte(nil), binary.BigEndian.AppendUint64(nil, state.Expires)...), queueIDBytes...), groupIDBytes...)
		for _, indexed := range []struct {
			index mdb.DBI
			key   []byte
		}{
			{db.pending.i, pendingKey},
			{db.active.i, activeKey},
			{db.expire.i, expireKey},
		} {
			if err := txn.Del(indexed.index, indexed.key, messageIDBytes); err != nil && !mdb.IsNotFound(err) {
				return err
			}
		}
		if err := txn.Del(db.attr.i, primaryKey, nil); err != nil {
			return err
		}
		if err := txn.Del(db.data.i, primaryKey, nil); err != nil && !mdb.IsNotFound(err) {
			return err
		}
	}
	return nil
}
