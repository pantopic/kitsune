package main

import (
	"github.com/pantopic/ext-mdb/sdk-go"
	"github.com/pantopic/ext-raft/sdk-go"

	"github.com/pantopic/kitsune/module/pkg/cmd"
	"github.com/pantopic/kitsune/module/pkg/globals"
	"github.com/pantopic/kitsune/module/pkg/pb"
	"github.com/pantopic/kitsune/module/pkg/query"
)

var (
	epoch    uint64
	newIndex uint64
	txn      mdb.Txn
)

func init() {
	raft.Persistent(open, read, update, commit, abort)
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

func update(index uint64, c []byte) (value uint64, data []byte) {
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
	switch c[len(c)-1] {
	case cmd.QUEUE_CREATE:
		req := &pb.CreateQueueRequest{}
		if err = req.UnmarshalVT(c[:len(c)-1]); err != nil {
			return 400, []byte(`Invalid CreateQueue request`)
		}
		q, err := dbQueue.createInTxn(txn, []byte(req.QueueName), req.Attributes)
		if err != nil {
			return 400, []byte(err.Error())
		}
		data, err = (&pb.CreateQueueResponse{QueueName: string(q.name)}).MarshalVT()
		if err != nil {
			panic(`Unable to marshal response: ` + err.Error())
		}
		value = 200
	case cmd.QUEUE_SET_ATTRIBUTES:
		req := &pb.SetQueueAttributesRequest{}
		if err = req.UnmarshalVT(c[:len(c)-1]); err != nil {
			return 400, []byte(`Invalid SetQueueAttributes request`)
		}
		if err = dbQueue.setAttributes(txn, []byte(req.QueueName), req.Attributes); err != nil {
			return 400, []byte(err.Error())
		}
		value = 200
	case cmd.QUEUE_DELETE:
		req := &pb.DeleteQueueRequest{}
		if err = req.UnmarshalVT(c[:len(c)-1]); err != nil {
			return 400, []byte(`Invalid DeleteQueue request`)
		}
		if err = dbQueue.delete(txn, []byte(req.QueueName)); err != nil {
			return 400, []byte(err.Error())
		}
		value = 200
	case cmd.QUEUE_PURGE:
		req := &pb.DeleteQueueRequest{}
		if err = req.UnmarshalVT(c[:len(c)-1]); err != nil {
			return 400, []byte(`Invalid PurgeQueue request`)
		}
		if err = dbQueue.purge(txn, []byte(req.QueueName)); err != nil {
			return 400, []byte(err.Error())
		}
		value = 200
	case cmd.MESSAGE_SEND:
		req := &pb.SendMessageRequest{}
		if err = req.UnmarshalVT(c[:len(c)-1]); err != nil {
			return 400, []byte(`Invalid SendMessage request`)
		}
		resp, sendErr := dbMessage.send(txn, req, epoch)
		if sendErr != nil {
			return 400, []byte(sendErr.Error())
		}
		data, err = resp.MarshalVT()
		if err != nil {
			return 500, []byte(err.Error())
		}
		value = 200
	case cmd.MESSAGE_DELETE:
		req := &pb.DeleteMessageRequest{}
		if err = req.UnmarshalVT(c[:len(c)-1]); err != nil {
			return 400, []byte(`Invalid DeleteMessage request`)
		}
		if err = dbMessage.delete(txn, req); err != nil {
			return 400, []byte(err.Error())
		}
		value = 200
	case cmd.MESSAGE_REFRESH:
		req := &pb.ChangeMessageVisibilityRequest{}
		if err = req.UnmarshalVT(c[:len(c)-1]); err != nil {
			return 400, []byte(`Invalid ChangeMessageVisibility request`)
		}
		if err = dbMessage.refresh(txn, req, epoch); err != nil {
			return 400, []byte(err.Error())
		}
		value = 200
	case cmd.MESSAGE_RECEIVE:
		req := &pb.ReceiveMessageRequest{}
		if err = req.UnmarshalVT(c[:len(c)-1]); err != nil {
			return 400, []byte(`Invalid ReceiveMessage request`)
		}
		resp, receiveErr := dbMessage.receive(txn, req, epoch)
		if receiveErr != nil {
			return 400, []byte(receiveErr.Error())
		}
		data, err = resp.MarshalVT()
		if err != nil {
			return 500, []byte(err.Error())
		}
		value = 200
	case cmd.INTERNAL_TICK:
		var req = &pb.TickRequest{}
		if err = req.UnmarshalVT(c[:len(c)-1]); err != nil {
			data = []byte(`Invalid command: ` + string(c))
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
		if err = dbMessage.tick(txn, epoch); err != nil {
			panic(`Unable to process message tick: ` + err.Error())
		}
		data, err = (&pb.TickResponse{
			Epoch: epoch,
		}).MarshalVT()
		if err != nil {
			panic(`Unable to marshal response: ` + err.Error())
		}
		value = index
	case cmd.INTERNAL_TERM:
		var req = &pb.TermRequest{}
		if err = req.UnmarshalVT(c[:len(c)-1]); err != nil {
			data = []byte(`Invalid command: ` + string(c))
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
		data, err = (&pb.TermResponse{}).MarshalVT()
		if err != nil {
			panic(`Unable to marshal response: ` + err.Error())
		}
		value = index
	default:
		return 500, []byte(`invalid cmd`)
	}
	return
}

func commit() {
	var err error
	if err = dbMeta.setIndex(txn, newIndex); err != nil {
		panic(`Unable to set index: ` + err.Error())
	}
	if err := txn.Commit(); err != nil {
		panic(`Unable to commit transaction: ` + err.Error())
	}
	txn = 0
}

func abort() {}

func read(q []byte) (val uint64, data []byte) {
	var code byte
	if len(q) > 0 {
		code = q[len(q)-1]
	}
	switch code {
	case query.HEALTH_CHECK:
		return 200, []byte(`Okay`)
	case query.QUEUE_LIST:
		req := &pb.ListQueuesRequest{}
		if err := req.UnmarshalVT(q[:len(q)-1]); err != nil {
			return 400, []byte(`Invalid ListQueues request`)
		}
		maxResults := req.MaxResults
		if maxResults == 0 || maxResults > globals.MAX_QUEUE_LIST_RESULTS() {
			maxResults = globals.MAX_QUEUE_LIST_RESULTS()
		}
		var names [][]byte
		err := mdb.View(func(txn mdb.Txn) error {
			var err error
			names, err = dbQueue.list(txn, []byte(req.QueueNamePrefix), maxResults)
			return err
		})
		if err != nil {
			return 500, []byte(err.Error())
		}
		resp := &pb.QueueListResponse{}
		for _, name := range names {
			resp.QueueNames = append(resp.QueueNames, string(name))
		}
		data, err := resp.MarshalVT()
		if err != nil {
			return 500, []byte(err.Error())
		}
		return 200, data
	case query.QUEUE_URL:
		req := &pb.GetQueueUrlRequest{}
		if err := req.UnmarshalVT(q[:len(q)-1]); err != nil {
			return 400, []byte(`Invalid GetQueueUrl request`)
		}
		err := mdb.View(func(txn mdb.Txn) error {
			_, err := dbQueue.getID(txn, []byte(req.QueueName))
			return err
		})
		if mdb.IsNotFound(err) {
			return 400, []byte(ErrQueueDoesNotExist.Error())
		}
		if err != nil {
			return 500, []byte(err.Error())
		}
		resp := &pb.GetQueueUrlResponse{QueueName: req.QueueName}
		data, err := resp.MarshalVT()
		if err != nil {
			return 500, []byte(err.Error())
		}
		return 200, data
	case query.QUEUE_ATTRIBUTES:
		req := &pb.GetQueueAttributesRequest{}
		if err := req.UnmarshalVT(q[:len(q)-1]); err != nil {
			return 400, []byte(`Invalid GetQueueAttributes request`)
		}
		var attributes map[string]string
		err := mdb.View(func(txn mdb.Txn) error {
			var err error
			attributes, err = dbQueue.getAttributes(txn, []byte(req.QueueName))
			return err
		})
		if mdb.IsNotFound(err) {
			return 400, []byte(ErrQueueDoesNotExist.Error())
		}
		if err != nil {
			if err == ErrQueueDoesNotExist {
				return 400, []byte(err.Error())
			}
			return 500, []byte(err.Error())
		}
		selected := make(map[string]string)
		if containsAttribute(req.AttributeNames, "All") {
			selected = attributes
		} else {
			for _, name := range req.AttributeNames {
				value, ok := attributes[name]
				if !ok {
					return 400, []byte(ErrQueueAttributeNameInvalid.Error())
				}
				selected[name] = value
			}
		}
		data, err := (&pb.GetQueueAttributesResponse{Attributes: selected}).MarshalVT()
		if err != nil {
			return 500, []byte(err.Error())
		}
		return 200, data
	case query.QUEUE_DEAD_LETTER_SOURCE:
		// Find queue
		// Return queue dead letter sources
	default:
		return 400, []byte(`unrecognized query`)
	}
	return 500, []byte(`unreachable`)
}

func containsAttribute(names []string, name string) bool {
	for _, current := range names {
		if current == name {
			return true
		}
	}
	return false
}
