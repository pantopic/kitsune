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
	ErrMessageBodyInvalid     = errors.New(`Message body invalid`)
	ErrMessageBodyTooLarge    = errors.New(`Message body too large`)
	ErrMessageDelayInvalid    = errors.New(`Message delay invalid`)
	ErrReceiptHandleInvalid   = errors.New(`Receipt handle is invalid`)
	ErrMessageBatchLimit      = errors.New(`Too many messages requested`)
	ErrReceiptLimit           = errors.New(`Message receipt generation exhausted`)
	ErrLongPollingUnsupported = errors.New(`Long polling is not supported`)
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

func (db dbMessageImpl) counts(txn mdb.Txn, queueID, epoch uint64) (visible, delayed, active uint64, err error) {
	queueKey := binary.BigEndian.AppendUint64(nil, queueID)
	cur, err := txn.OpenCursor(db.pending.i)
	if err != nil {
		return 0, 0, 0, err
	}
	key, value, err := cur.Get(queueKey, nil, mdb.SetRange)
	for err == nil && len(key) >= 8 && binary.BigEndian.Uint64(key[:8]) == queueID {
		if len(key) != 24 || len(value) != 8 {
			cur.Close()
			return 0, 0, 0, ErrKeyInvalid
		}
		if binary.BigEndian.Uint64(key[8:16]) > epoch {
			delayed++
		} else {
			visible++
		}
		key, value, err = cur.Get(key, value, mdb.Next)
	}
	cur.Close()
	if err != nil && !mdb.IsNotFound(err) {
		return 0, 0, 0, err
	}
	cur, err = txn.OpenCursor(db.active.i)
	if err != nil {
		return 0, 0, 0, err
	}
	key, value, err = cur.Get(binary.BigEndian.AppendUint64(nil, epoch), nil, mdb.SetRange)
	for err == nil {
		if len(key) != 24 || len(value) != 8 {
			cur.Close()
			return 0, 0, 0, ErrKeyInvalid
		}
		if binary.BigEndian.Uint64(key[:8]) > epoch && binary.BigEndian.Uint64(key[8:16]) == queueID {
			active++
		}
		key, value, err = cur.Get(key, value, mdb.Next)
	}
	cur.Close()
	if err != nil && !mdb.IsNotFound(err) {
		return 0, 0, 0, err
	}
	return visible, delayed, active, nil
}

func (db dbMessageImpl) send(txn mdb.Txn, req *pb.SendMessageRequest, epoch uint64) (*pb.SendMessageResponse, error) {
	if !validMessageBody(req.MessageBody) {
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
	if delay == 0 && !req.DelaySecondsSet {
		delay, err = strconv.ParseUint(attributes["DelaySeconds"], 10, 64)
		if err != nil {
			return nil, err
		}
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

func validMessageBody(body string) bool {
	if body == "" || !utf8.ValidString(body) {
		return false
	}
	for _, r := range body {
		if r != '\t' && r != '\n' && r != '\r' &&
			(r < 0x20 || r > 0x10ffff || r >= 0xd800 && r <= 0xdfff || r == 0xfffe || r == 0xffff) {
			return false
		}
	}
	return true
}

func (db dbMessageImpl) sendBatch(txn mdb.Txn, req *pb.SendMessageBatchRequest, epoch uint64) (*pb.SendMessageBatchResponse, string, error) {
	ids := make([]string, 0, len(req.Entries))
	var size uint64
	for _, entry := range req.Entries {
		if entry == nil {
			ids = append(ids, "")
			continue
		}
		ids = append(ids, entry.Id)
		size += uint64(len(entry.MessageBody))
	}
	if code := validateBatchIDs(ids); code != "" {
		return nil, code, nil
	}
	if size > globals.MAX_MESSAGE_SIZE() {
		return nil, "BatchRequestTooLong", nil
	}
	if _, err := dbQueue.getID(txn, []byte(req.QueueName)); err != nil {
		if mdb.IsNotFound(err) {
			return nil, "QueueDoesNotExist", nil
		}
		return nil, "", err
	}
	resp := &pb.SendMessageBatchResponse{}
	err := txn.Sub(func(batchTxn mdb.Txn) error {
		for _, entry := range req.Entries {
			var code, message string
			switch {
			case entry.UnsupportedFields:
				code, message = "UnsupportedOperation", "Message attributes and FIFO fields are not supported"
			case !entry.MessageBodySet:
				code, message = "InvalidParameterValue", "MessageBody is required"
			case entry.MessageBody == "":
				code, message = "InvalidParameterValue", "MessageBody must not be empty"
			default:
				sent, err := db.send(batchTxn, &pb.SendMessageRequest{
					QueueName:       req.QueueName,
					MessageBody:     entry.MessageBody,
					DelaySeconds:    entry.DelaySeconds,
					DelaySecondsSet: entry.DelaySecondsSet,
				}, epoch)
				switch err {
				case nil:
					resp.Successful = append(resp.Successful, &pb.SendMessageBatchResultEntry{
						Id: entry.Id, MessageId: sent.MessageId, Md5OfMessageBody: sent.Md5OfMessageBody,
					})
				case ErrMessageBodyInvalid:
					code, message = "InvalidMessageContents", err.Error()
				case ErrMessageBodyTooLarge, ErrMessageDelayInvalid:
					code, message = "InvalidParameterValue", err.Error()
				default:
					return err
				}
			}
			if code != "" {
				resp.Failed = append(resp.Failed, &pb.BatchResultErrorEntry{
					Id: entry.Id, Code: code, Message: message, SenderFault: true,
				})
			}
		}
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	return resp, "", nil
}

func validateBatchIDs(ids []string) string {
	if len(ids) == 0 {
		return "EmptyBatchRequest"
	}
	if uint64(len(ids)) > globals.MAX_MESSAGE_BATCH_SIZE() {
		return "TooManyEntriesInBatchRequest"
	}
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if !validBatchID(id) {
			return "InvalidBatchEntryId"
		}
		if seen[id] {
			return "BatchEntryIdsNotDistinct"
		}
		seen[id] = true
	}
	return ""
}

func (db dbMessageImpl) receiptBatch(txn mdb.Txn, req *pb.MessageReceiptBatchRequest, epoch uint64, refresh bool) (*pb.MessageReceiptBatchResponse, string, error) {
	ids := make([]string, 0, len(req.Entries))
	for _, entry := range req.Entries {
		if entry == nil {
			ids = append(ids, "")
		} else {
			ids = append(ids, entry.Id)
		}
	}
	if code := validateBatchIDs(ids); code != "" {
		return nil, code, nil
	}
	if _, err := dbQueue.getID(txn, []byte(req.QueueName)); err != nil {
		if mdb.IsNotFound(err) {
			return nil, "QueueDoesNotExist", nil
		}
		return nil, "", err
	}
	resp := &pb.MessageReceiptBatchResponse{}
	err := txn.Sub(func(batchTxn mdb.Txn) error {
		for _, entry := range req.Entries {
			var entryErr error
			if refresh {
				if !entry.VisibilityTimeoutSet {
					entryErr = ErrQueueAttributeValueInvalid
				} else {
					entryErr = db.refresh(batchTxn, &pb.ChangeMessageVisibilityRequest{
						QueueName: req.QueueName, ReceiptHandle: entry.ReceiptHandle, VisibilityTimeout: entry.VisibilityTimeout,
					}, epoch)
				}
			} else {
				entryErr = db.delete(batchTxn, &pb.DeleteMessageRequest{QueueName: req.QueueName, ReceiptHandle: entry.ReceiptHandle})
			}
			switch entryErr {
			case nil:
				resp.Successful = append(resp.Successful, &pb.MessageReceiptBatchResultEntry{Id: entry.Id})
			case ErrReceiptHandleInvalid, ErrQueueAttributeValueInvalid:
				code := "ReceiptHandleIsInvalid"
				if entryErr == ErrQueueAttributeValueInvalid {
					code = "InvalidParameterValue"
				}
				resp.Failed = append(resp.Failed, &pb.BatchResultErrorEntry{Id: entry.Id, Code: code, Message: entryErr.Error(), SenderFault: true})
			default:
				return entryErr
			}
		}
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	return resp, "", nil
}

func validBatchID(id string) bool {
	if len(id) == 0 || len(id) > 80 {
		return false
	}
	for _, ch := range id {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_') {
			return false
		}
	}
	return true
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
	wait := req.WaitTimeSeconds
	if !req.WaitTimeSecondsSet {
		wait, err = strconv.ParseUint(queueAttributes["ReceiveMessageWaitTimeSeconds"], 10, 64)
		if err != nil {
			return nil, err
		}
	}
	if wait > globals.MAX_POLL_WAIT_TIME() {
		return nil, ErrQueueAttributeValueInvalid
	}
	if wait > 0 {
		return nil, ErrLongPollingUnsupported
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
		if state.ReceiptGeneration == ^uint64(0) {
			return nil, ErrReceiptLimit
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

		previousActiveKey := append(append(append([]byte(nil), binary.BigEndian.AppendUint64(nil, state.ActiveUntil)...), queueIDBytes...), groupIDBytes...)
		if err := txn.Del(db.active.i, previousActiveKey, messageIDBytes); err != nil && !mdb.IsNotFound(err) {
			return nil, err
		}

		timeout := epoch + visibility
		activeKey := append(append(append([]byte(nil), binary.BigEndian.AppendUint64(nil, timeout)...), queueIDBytes...), groupIDBytes...)
		if err := txn.Put(db.active.i, activeKey, messageIDBytes, 0); err != nil {
			return nil, err
		}
		state.ReceiveCount++
		state.ActiveUntil = timeout
		state.ReceiptGeneration++
		state.ReceiptTimeout = timeout
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
		receipt = binary.BigEndian.AppendUint64(receipt, state.ReceiptGeneration)
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
		activeKey := append(append(append([]byte(nil), binary.BigEndian.AppendUint64(nil, state.ActiveUntil)...), queueIDBytes...), groupIDBytes...)
		if err := txn.Del(db.active.i, activeKey, messageID); err != nil && !mdb.IsNotFound(err) {
			return err
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

func (db dbMessageImpl) lookupReceipt(txn mdb.Txn, queueName, handle string) ([]byte, *pb.StoredMessage, []byte, error) {
	receipt, err := hex.DecodeString(handle)
	if err != nil || (len(receipt) != 32 && len(receipt) != 40) {
		return nil, nil, nil, ErrReceiptHandleInvalid
	}
	queueID, err := dbQueue.getID(txn, []byte(queueName))
	if mdb.IsNotFound(err) || (err == nil && queueID != binary.BigEndian.Uint64(receipt[:8])) {
		return nil, nil, nil, ErrReceiptHandleInvalid
	}
	if err != nil {
		return nil, nil, nil, err
	}
	primaryKey := receipt[:24]
	stateBytes, err := txn.Get(db.attr.i, primaryKey, nil)
	if mdb.IsNotFound(err) {
		return nil, nil, nil, ErrReceiptHandleInvalid
	}
	if err != nil {
		return nil, nil, nil, err
	}
	stateBytes, err = db.attr.trimChecksum(primaryKey, stateBytes)
	if err != nil {
		return nil, nil, nil, err
	}
	state := &pb.StoredMessage{}
	if err := state.UnmarshalVT(stateBytes); err != nil {
		return nil, nil, nil, err
	}
	timeout := binary.BigEndian.Uint64(receipt[24:32])
	if len(receipt) == 32 {
		if state.ReceiptGeneration != 0 || state.ActiveUntil != timeout {
			return nil, nil, nil, ErrReceiptHandleInvalid
		}
	} else if state.ReceiptGeneration == 0 || state.ReceiptGeneration != binary.BigEndian.Uint64(receipt[32:]) || state.ReceiptTimeout != timeout {
		return nil, nil, nil, ErrReceiptHandleInvalid
	}
	messageIDBytes := receipt[16:24]
	activeKey := append(binary.BigEndian.AppendUint64(nil, state.ActiveUntil), receipt[:16]...)
	cur, err := txn.OpenCursor(db.active.i)
	if err != nil {
		return nil, nil, nil, err
	}
	_, _, err = cur.Get(activeKey, messageIDBytes, mdb.GetBoth)
	cur.Close()
	if mdb.IsNotFound(err) {
		return nil, nil, nil, ErrReceiptHandleInvalid
	}
	if err != nil {
		return nil, nil, nil, err
	}
	return receipt, state, activeKey, nil
}

func (db dbMessageImpl) delete(txn mdb.Txn, req *pb.DeleteMessageRequest) error {
	receipt, state, activeKey, err := db.lookupReceipt(txn, req.QueueName, req.ReceiptHandle)
	if err != nil {
		return err
	}
	messageIDBytes := receipt[16:24]
	if err := txn.Del(db.active.i, activeKey, messageIDBytes); err != nil {
		return err
	}
	primaryKey := append([]byte(nil), receipt[:24]...)
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
	receipt, state, activeKey, err := db.lookupReceipt(txn, req.QueueName, req.ReceiptHandle)
	if err != nil {
		return err
	}
	messageIDBytes := receipt[16:24]
	primaryKey := append([]byte(nil), receipt[:24]...)
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
	stateBytes, err := state.MarshalVT()
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
