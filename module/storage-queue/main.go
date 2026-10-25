package main

import (
	"github.com/pantopic/ext-mdb/sdk-go"
	"github.com/pantopic/ext-raft/sdk-go"
	"github.com/pantopic/ext-range-watch/sdk-go"

	internal "github.com/pantopic/queue/module/storage-queue/internal"
)

const (
	codeNotFound uint64 = 5
)

var (
	epoch    uint64
	newIndex uint64
	txn      mdb.Txn
)

func init() {
	raft.Persistent(open, update, finish, read)
}

func main() {}

func open() (index uint64) {
	if err := mdb.Update(func(txn mdb.Txn) (err error) {
		index = dbMeta.init(txn)
		dbQueue.init(txn)
		dbGroup.init(txn)
		dbMessage.init(txn)
		return nil
	}); err != nil {
		panic(`Unable to open env ` + err.Error())
	}
	return
}

func update(index uint64, cmd []byte) (value uint64, data []byte) {
	newIndex = index
	var err error
	if txn == 0 {
		txn, err = mdb.Begin(0)
		if err != nil {
			panic(`Unable to open txn: ` + err.Error())
		}
		epoch, err = dbMeta.getEpoch(txn)
		if err != nil {
			panic(`Unable to get epoch: ` + err.Error())
		}
	}
	switch cmd[len(cmd)-1] {
	case CMD_QUEUE_CREATE:
		// Incr queue_id
		// Insert queue_id
		// Insert queue
		// Insert queue_stat
	case CMD_QUEUE_PURGE:
		// Find queue
		//   else return error
		// Delete message (prefix queue_id)
	case CMD_QUEUE_DELETE:
		// Find queue
		//   else return error
		// Delete queue_id
		// Delete queue
		// Delete queue_stat
		// Delete message (prefix queue_id)
	case CMD_MESSAGE_SEND:
		// Find queue
		//   else return error
		// Find group
		//   else insert group, group_name, group_stat
		// Insert message
		// Insert data
	case CMD_MESSAGE_DELETE:
		// Find message
		//   else return error
		// Delete message
		// Delete message_active
		// Delete message_expire
	case CMD_MESSAGE_RECEIVE:
		// Find queue
		//   else return error
		// Scan message (prefix queue_id)
		//   else return wait
		// Delete message
		// Insert message_active
		// Insert message_expire
		// Return data
	case CMD_INTERNAL_TICK:
		var req = &internal.TickRequest{}
		if err = req.UnmarshalVT(cmd[:len(cmd)-1]); err != nil {
			data = []byte(`Invalid command: ` + string(cmd))
			return
		}
		term, err := dbMeta.getTerm(txn)
		if err != nil {
			panic(`Unable to get term: ` + err.Error())
		}
		if term > req.Term {
			data = []byte(ErrTermExpired.Error())
			return
		}
		epoch++
		if err = dbMeta.setEpoch(txn, epoch); err != nil {
			panic(`Unable to set epoch: ` + err.Error())
		}
		// message recycle
		// message expire
		data, err = (&internal.TickResponse{
			Epoch: epoch,
		}).MarshalVT()
		if err != nil {
			panic(`Unable to marshal response: ` + err.Error())
		}
		value = index
	case CMD_INTERNAL_TERM:
		var req = &internal.TermRequest{}
		if err = req.UnmarshalVT(cmd[:len(cmd)-1]); err != nil {
			data = []byte(`Invalid command: ` + string(cmd))
			return
		}
		term, err := dbMeta.getTerm(txn)
		if err != nil {
			panic(`Unable to get term: ` + err.Error())
		}
		if term > req.Term {
			data = []byte(ErrTermExpired.Error())
			return
		}
		if err = dbMeta.setTerm(txn, req.Term); err != nil {
			panic(`Unable to set term: ` + err.Error())
		}
		data, err = (&internal.TermResponse{}).MarshalVT()
		if err != nil {
			panic(`Unable to marshal response: ` + err.Error())
		}
		value = index
	}
	return
}

func finish() {
	var err error
	if err = dbMeta.setIndex(txn, newIndex); err != nil {
		panic(`Unable to set index: ` + err.Error())
	}
	if err := txn.Commit(); err != nil {
		panic(`Unable to commit transaction: ` + err.Error())
	}
	range_watch.Flush()
	txn = 0
}

func read(query []byte) (value uint64, data []byte) {
	switch query[len(query)-1] {
	case QUERY_QUEUE_LIST:
		// Scan queues
		// Return list of queues
	case QUERY_QUEUE_URL:
		// Find queue
		// Return queue url
	case QUERY_QUEUE_ATTRIBUTES:
		// Find queue
		// Return queue attributes
	case QUERY_QUEUE_DEAD_LETTER_SOURCE:
		// Find queue
		// Return queue dead letter sources
	}
	return
}
