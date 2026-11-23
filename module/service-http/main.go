package main

import (
	"encoding/json"
	"strings"

	"github.com/pantopic/ext-http-server/sdk-go"
	"github.com/pantopic/ext-shard-client/sdk-go"

	"github.com/pantopic/kitsune/module/pkg/cmd"
	"github.com/pantopic/kitsune/module/pkg/pb"
	"github.com/pantopic/kitsune/module/pkg/query"
)

var (
	bodyBuf        = make([]byte, 1<<20)
	request        = &pb.Request{}
	reqCreateQueue = &pb.CreateQueueRequest{}
	reqGetQueueURL = &pb.GetQueueUrlRequest{}
)

type deleteQueueRequest struct {
	QueueURL string `json:"QueueUrl"`
}

type listQueuesRequest struct {
	MaxResults      *uint64 `json:"MaxResults"`
	NextToken       string  `json:"NextToken"`
	QueueNamePrefix string  `json:"QueueNamePrefix"`
}

type setQueueAttributesRequest struct {
	QueueURL   string            `json:"QueueUrl"`
	Attributes map[string]string `json:"Attributes"`
}

type getQueueAttributesRequest struct {
	QueueURL       string   `json:"QueueUrl"`
	AttributeNames []string `json:"AttributeNames"`
}

type sendMessageRequest struct {
	QueueURL                string                     `json:"QueueUrl"`
	MessageBody             string                     `json:"MessageBody"`
	DelaySeconds            *uint64                    `json:"DelaySeconds"`
	MessageAttributes       map[string]json.RawMessage `json:"MessageAttributes"`
	MessageSystemAttributes map[string]json.RawMessage `json:"MessageSystemAttributes"`
	MessageDeduplicationID  string                     `json:"MessageDeduplicationId"`
	MessageGroupID          string                     `json:"MessageGroupId"`
}

type sendMessageBatchRequest struct {
	QueueURL string `json:"QueueUrl"`
	Entries  []struct {
		ID                      string                     `json:"Id"`
		MessageBody             *string                    `json:"MessageBody"`
		DelaySeconds            *uint64                    `json:"DelaySeconds"`
		MessageAttributes       map[string]json.RawMessage `json:"MessageAttributes"`
		MessageSystemAttributes map[string]json.RawMessage `json:"MessageSystemAttributes"`
		MessageDeduplicationID  string                     `json:"MessageDeduplicationId"`
		MessageGroupID          string                     `json:"MessageGroupId"`
	} `json:"Entries"`
}

type receiveMessageRequest struct {
	QueueURL                    string   `json:"QueueUrl"`
	MaxNumberOfMessages         uint64   `json:"MaxNumberOfMessages"`
	VisibilityTimeout           *uint64  `json:"VisibilityTimeout"`
	WaitTimeSeconds             *uint64  `json:"WaitTimeSeconds"`
	MessageAttributeNames       []string `json:"MessageAttributeNames"`
	MessageSystemAttributeNames []string `json:"MessageSystemAttributeNames"`
	AttributeNames              []string `json:"AttributeNames"`
}

type deleteMessageRequest struct {
	QueueURL      string `json:"QueueUrl"`
	ReceiptHandle string `json:"ReceiptHandle"`
}

type receiptBatchRequest struct {
	QueueURL string `json:"QueueUrl"`
	Entries  []struct {
		ID                string  `json:"Id"`
		ReceiptHandle     string  `json:"ReceiptHandle"`
		VisibilityTimeout *uint64 `json:"VisibilityTimeout"`
	} `json:"Entries"`
}

type changeMessageVisibilityRequest struct {
	QueueURL          string `json:"QueueUrl"`
	ReceiptHandle     string `json:"ReceiptHandle"`
	VisibilityTimeout uint64 `json:"VisibilityTimeout"`
}

func init() {
	shard_client.RegisterAsyncRecv(asyncRecv)
	http_server.Init(
		http_server.WithHandler(httpHandler),
	)
}

func main() {
}

func httpHandler(method, path []byte) {
	switch string(path) {
	case "/":
		contentType := http_server.GetHeaders("content-type")
		if contentType["content-type"] != "application/x-amz-json-1.0" {
			http_server.Send(400, []byte(`invalid content-type`))
			return
		}
		host := http_server.GetHeaders("x-forwarded-host")
		requestHost := host["x-forwarded-host"]
		if requestHost == "" {
			requestHost = "localhost"
		}
		targetHeader := http_server.GetHeaders("x-amz-target")
		var body []byte
		if http_server.BodyLen() > cap(bodyBuf) {
			body = http_server.BodyRead(nil)
		} else {
			body = http_server.BodyRead(bodyBuf)
		}
		request = &pb.Request{}
		if err := request.UnmarshalJSON(body); err != nil {
			http_server.Send(400, []byte(err.Error()))
			return
		}
		action := request.Action
		if target := targetHeader["x-amz-target"]; target != "" {
			if i := strings.LastIndexByte(target, '.'); i >= 0 {
				action = target[i+1:]
			} else {
				action = target
			}
		}
		switch action {
		case "ListQueues":
			var req listQueuesRequest
			if err := json.Unmarshal(body, &req); err != nil {
				http_server.Send(400, []byte(err.Error()))
				return
			}
			listReq := &pb.ListQueuesRequest{NextToken: req.NextToken, QueueNamePrefix: req.QueueNamePrefix}
			if req.MaxResults != nil {
				listReq.MaxResults = *req.MaxResults
				listReq.MaxResultsSet = true
			}
			b, err := listReq.MarshalVT()
			if err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
			if err := queueShard().ReadAsync(asyncName(action, requestHost), append(b, query.QUEUE_LIST), false); err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
		case "CreateQueue":
			reqCreateQueue = &pb.CreateQueueRequest{}
			if err := reqCreateQueue.UnmarshalJSON(body); err != nil {
				http_server.Send(400, []byte(err.Error()))
				return
			}
			b, err := reqCreateQueue.MarshalVT()
			if err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
			if err := queueShard().ApplyAsync(asyncName(action, requestHost), append(b, cmd.QUEUE_CREATE)); err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
		case "GetQueueUrl":
			reqGetQueueURL = &pb.GetQueueUrlRequest{}
			if err := reqGetQueueURL.UnmarshalJSON(body); err != nil {
				http_server.Send(400, []byte(err.Error()))
				return
			}
			b, err := reqGetQueueURL.MarshalVT()
			if err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
			if err := queueShard().ReadAsync(asyncName(action, requestHost), append(b, query.QUEUE_URL), false); err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
		case "DeleteQueue":
			var req deleteQueueRequest
			if err := json.Unmarshal(body, &req); err != nil {
				http_server.Send(400, []byte(err.Error()))
				return
			}
			name, ok := queueNameFromURL(req.QueueURL)
			if !ok {
				http_server.Send(400, []byte(`invalid QueueUrl`))
				return
			}
			b, err := (&pb.DeleteQueueRequest{QueueName: name}).MarshalVT()
			if err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
			if err := queueShard().ApplyAsync(asyncName(action, requestHost), append(b, cmd.QUEUE_DELETE)); err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
		case "PurgeQueue":
			var req deleteQueueRequest
			if err := json.Unmarshal(body, &req); err != nil {
				http_server.Send(400, []byte(err.Error()))
				return
			}
			name, ok := queueNameFromURL(req.QueueURL)
			if !ok {
				http_server.Send(400, []byte(`invalid QueueUrl`))
				return
			}
			b, err := (&pb.DeleteQueueRequest{QueueName: name}).MarshalVT()
			if err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
			if err := queueShard().ApplyAsync(asyncName(action, requestHost), append(b, cmd.QUEUE_PURGE)); err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
		case "GetQueueAttributes":
			var req getQueueAttributesRequest
			if err := json.Unmarshal(body, &req); err != nil {
				http_server.Send(400, []byte(err.Error()))
				return
			}
			name, ok := queueNameFromURL(req.QueueURL)
			if !ok {
				http_server.Send(400, []byte(`invalid QueueUrl`))
				return
			}
			b, err := (&pb.GetQueueAttributesRequest{
				QueueName:      name,
				AttributeNames: req.AttributeNames,
			}).MarshalVT()
			if err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
			if err := queueShard().ReadAsync(asyncName(action, requestHost), append(b, query.QUEUE_ATTRIBUTES), false); err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
		case "SetQueueAttributes":
			var req setQueueAttributesRequest
			if err := json.Unmarshal(body, &req); err != nil {
				http_server.Send(400, []byte(err.Error()))
				return
			}
			name, ok := queueNameFromURL(req.QueueURL)
			if !ok {
				http_server.Send(400, []byte(`invalid QueueUrl`))
				return
			}
			b, err := (&pb.SetQueueAttributesRequest{
				QueueName:  name,
				Attributes: req.Attributes,
			}).MarshalVT()
			if err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
			if err := queueShard().ApplyAsync(asyncName(action, requestHost), append(b, cmd.QUEUE_SET_ATTRIBUTES)); err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
		case "SendMessage":
			var req sendMessageRequest
			if err := json.Unmarshal(body, &req); err != nil {
				http_server.Send(400, []byte(err.Error()))
				return
			}
			if len(req.MessageAttributes) > 0 || len(req.MessageSystemAttributes) > 0 || req.MessageDeduplicationID != "" || req.MessageGroupID != "" {
				sendUnsupported("Message attributes and FIFO fields are not supported")
				return
			}
			name, ok := queueNameFromURL(req.QueueURL)
			if !ok {
				http_server.Send(400, []byte(`invalid QueueUrl`))
				return
			}
			sendReq := &pb.SendMessageRequest{QueueName: name, MessageBody: req.MessageBody}
			if req.DelaySeconds != nil {
				sendReq.DelaySeconds = *req.DelaySeconds
				sendReq.DelaySecondsSet = true
			}
			b, err := sendReq.MarshalVT()
			if err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
			if err := queueShard().ApplyAsync(asyncName(action, requestHost), append(b, cmd.MESSAGE_SEND)); err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
		case "SendMessageBatch":
			var req sendMessageBatchRequest
			if err := json.Unmarshal(body, &req); err != nil {
				http_server.Send(400, []byte(err.Error()))
				return
			}
			name, ok := queueNameFromURL(req.QueueURL)
			if !ok {
				http_server.Send(400, []byte(`invalid QueueUrl`))
				return
			}
			batch := &pb.SendMessageBatchRequest{QueueName: name}
			for _, entry := range req.Entries {
				item := &pb.SendMessageBatchRequestEntry{
					Id:                entry.ID,
					MessageBodySet:    entry.MessageBody != nil,
					DelaySecondsSet:   entry.DelaySeconds != nil,
					UnsupportedFields: len(entry.MessageAttributes) > 0 || len(entry.MessageSystemAttributes) > 0 || entry.MessageDeduplicationID != "" || entry.MessageGroupID != "",
				}
				if entry.MessageBody != nil {
					item.MessageBody = *entry.MessageBody
				}
				if entry.DelaySeconds != nil {
					item.DelaySeconds = *entry.DelaySeconds
				}
				batch.Entries = append(batch.Entries, item)
			}
			b, err := batch.MarshalVT()
			if err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
			if err := queueShard().ApplyAsync(asyncName(action, requestHost), append(b, cmd.MESSAGE_SEND_BATCH)); err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
		case "ReceiveMessage":
			var req receiveMessageRequest
			if err := json.Unmarshal(body, &req); err != nil {
				http_server.Send(400, []byte(err.Error()))
				return
			}
			if len(req.MessageAttributeNames) > 0 || len(req.MessageSystemAttributeNames) > 0 || len(req.AttributeNames) > 0 {
				sendUnsupported("Message attributes are not supported")
				return
			}
			name, ok := queueNameFromURL(req.QueueURL)
			if !ok {
				http_server.Send(400, []byte(`invalid QueueUrl`))
				return
			}
			var visibilityTimeout uint64
			if req.VisibilityTimeout != nil {
				visibilityTimeout = *req.VisibilityTimeout
			}
			receiveReq := &pb.ReceiveMessageRequest{
				QueueName:            name,
				MaxNumberOfMessages:  req.MaxNumberOfMessages,
				VisibilityTimeout:    visibilityTimeout,
				VisibilityTimeoutSet: req.VisibilityTimeout != nil,
				WaitTimeSecondsSet:   req.WaitTimeSeconds != nil,
			}
			if req.WaitTimeSeconds != nil {
				receiveReq.WaitTimeSeconds = *req.WaitTimeSeconds
			}
			b, err := receiveReq.MarshalVT()
			if err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
			if err := queueShard().ApplyAsync(asyncName(action, requestHost), append(b, cmd.MESSAGE_RECEIVE)); err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
		case "DeleteMessage":
			var req deleteMessageRequest
			if err := json.Unmarshal(body, &req); err != nil {
				http_server.Send(400, []byte(err.Error()))
				return
			}
			name, ok := queueNameFromURL(req.QueueURL)
			if !ok {
				http_server.Send(400, []byte(`invalid QueueUrl`))
				return
			}
			b, err := (&pb.DeleteMessageRequest{
				QueueName:     name,
				ReceiptHandle: req.ReceiptHandle,
			}).MarshalVT()
			if err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
			if err := queueShard().ApplyAsync(asyncName(action, requestHost), append(b, cmd.MESSAGE_DELETE)); err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
		case "DeleteMessageBatch", "ChangeMessageVisibilityBatch":
			var req receiptBatchRequest
			if err := json.Unmarshal(body, &req); err != nil {
				http_server.Send(400, []byte(err.Error()))
				return
			}
			name, ok := queueNameFromURL(req.QueueURL)
			if !ok {
				http_server.Send(400, []byte(`invalid QueueUrl`))
				return
			}
			batch := &pb.MessageReceiptBatchRequest{QueueName: name}
			for _, entry := range req.Entries {
				item := &pb.MessageReceiptBatchRequestEntry{
					Id: entry.ID, ReceiptHandle: entry.ReceiptHandle, VisibilityTimeoutSet: entry.VisibilityTimeout != nil,
				}
				if entry.VisibilityTimeout != nil {
					item.VisibilityTimeout = *entry.VisibilityTimeout
				}
				batch.Entries = append(batch.Entries, item)
			}
			b, err := batch.MarshalVT()
			if err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
			command := cmd.MESSAGE_DELETE_BATCH
			if action == "ChangeMessageVisibilityBatch" {
				command = cmd.MESSAGE_REFRESH_BATCH
			}
			if err := queueShard().ApplyAsync(asyncName(action, requestHost), append(b, command)); err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
		case "ChangeMessageVisibility":
			var req changeMessageVisibilityRequest
			if err := json.Unmarshal(body, &req); err != nil {
				http_server.Send(400, []byte(err.Error()))
				return
			}
			name, ok := queueNameFromURL(req.QueueURL)
			if !ok {
				http_server.Send(400, []byte(`invalid QueueUrl`))
				return
			}
			b, err := (&pb.ChangeMessageVisibilityRequest{
				QueueName:         name,
				ReceiptHandle:     req.ReceiptHandle,
				VisibilityTimeout: req.VisibilityTimeout,
			}).MarshalVT()
			if err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
			if err := queueShard().ApplyAsync(asyncName(action, requestHost), append(b, cmd.MESSAGE_REFRESH)); err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
		default:
			http_server.Send(400, []byte(`invalid action`))
			return
		}
	case "/health":
		if err := queueShard().ReadAsync([]byte(`health`), []byte{query.HEALTH_CHECK}, false); err != nil {
			http_server.Send(500, []byte(err.Error()))
			return
		}
	default:
		http_server.Send(404, []byte(`not found`))
	}
}

func asyncRecv(name, data []byte, val uint64, err error) {
	if err != nil {
		http_server.Send(500, []byte(err.Error()))
		return
	}
	action, host, hasHost := strings.Cut(string(name), "\x00")
	if !hasHost {
		action = string(name)
	}
	switch action {
	case "health":
		if val == 0 {
			http_server.Send(500, []byte(`Invalid response code`))
			return
		}
		http_server.Send(int(val), data)
	case "ListQueues":
		if val != 200 {
			http_server.Send(int(val), data)
			return
		}
		resp := &pb.QueueListResponse{}
		if err := resp.UnmarshalVT(data); err != nil {
			http_server.Send(500, []byte(err.Error()))
			return
		}
		urls := make([]string, 0, len(resp.QueueNames))
		for _, name := range resp.QueueNames {
			urls = append(urls, queueURL(name, host))
		}
		body, err := json.Marshal(struct {
			QueueURLs []string `json:"QueueUrls"`
			NextToken string   `json:"NextToken,omitempty"`
		}{QueueURLs: urls, NextToken: resp.NextToken})
		if err != nil {
			http_server.Send(500, []byte(err.Error()))
			return
		}
		sendJSON(200, body)
	case "CreateQueue":
		if val != 200 {
			http_server.Send(int(val), data)
			return
		}
		resp := &pb.CreateQueueResponse{}
		if err := resp.UnmarshalVT(data); err != nil {
			http_server.Send(500, []byte(err.Error()))
			return
		}
		body, err := json.Marshal(struct {
			QueueURL string `json:"QueueUrl"`
		}{QueueURL: queueURL(resp.QueueName, host)})
		if err != nil {
			http_server.Send(500, []byte(err.Error()))
			return
		}
		sendJSON(200, body)
	case "GetQueueUrl":
		if val != 200 {
			http_server.Send(int(val), data)
			return
		}
		resp := &pb.GetQueueUrlResponse{}
		if err := resp.UnmarshalVT(data); err != nil {
			http_server.Send(500, []byte(err.Error()))
			return
		}
		body, err := json.Marshal(struct {
			QueueURL string `json:"QueueUrl"`
		}{QueueURL: queueURL(resp.QueueName, host)})
		if err != nil {
			http_server.Send(500, []byte(err.Error()))
			return
		}
		sendJSON(200, body)
	case "DeleteQueue":
		if val != 200 {
			http_server.Send(int(val), data)
			return
		}
		sendJSON(200, []byte(`{}`))
	case "PurgeQueue":
		if val != 200 {
			http_server.Send(int(val), data)
			return
		}
		sendJSON(200, []byte(`{}`))
	case "GetQueueAttributes":
		if val != 200 {
			http_server.Send(int(val), data)
			return
		}
		resp := &pb.GetQueueAttributesResponse{}
		if err := resp.UnmarshalVT(data); err != nil {
			http_server.Send(500, []byte(err.Error()))
			return
		}
		body, err := json.Marshal(struct {
			Attributes map[string]string `json:"Attributes"`
		}{Attributes: resp.Attributes})
		if err != nil {
			http_server.Send(500, []byte(err.Error()))
			return
		}
		sendJSON(200, body)
	case "SetQueueAttributes":
		if val != 200 {
			http_server.Send(int(val), data)
			return
		}
		sendJSON(200, []byte(`{}`))
	case "SendMessage":
		if val != 200 {
			http_server.Send(int(val), data)
			return
		}
		resp := &pb.SendMessageResponse{}
		if err := resp.UnmarshalVT(data); err != nil {
			http_server.Send(500, []byte(err.Error()))
			return
		}
		body, err := json.Marshal(struct {
			MessageID        string `json:"MessageId"`
			Md5OfMessageBody string `json:"MD5OfMessageBody"`
		}{MessageID: resp.MessageId, Md5OfMessageBody: resp.Md5OfMessageBody})
		if err != nil {
			http_server.Send(500, []byte(err.Error()))
			return
		}
		sendJSON(200, body)
	case "SendMessageBatch":
		if sendBatchError(val, data) {
			return
		}
		resp := &pb.SendMessageBatchResponse{}
		if err := resp.UnmarshalVT(data); err != nil {
			http_server.Send(500, []byte(err.Error()))
			return
		}
		type successfulEntry struct {
			ID               string `json:"Id"`
			MessageID        string `json:"MessageId"`
			Md5OfMessageBody string `json:"MD5OfMessageBody"`
		}
		type failedEntry struct {
			ID          string `json:"Id"`
			Code        string `json:"Code"`
			Message     string `json:"Message"`
			SenderFault bool   `json:"SenderFault"`
		}
		successful := make([]successfulEntry, 0, len(resp.Successful))
		for _, entry := range resp.Successful {
			successful = append(successful, successfulEntry{ID: entry.Id, MessageID: entry.MessageId, Md5OfMessageBody: entry.Md5OfMessageBody})
		}
		failed := make([]failedEntry, 0, len(resp.Failed))
		for _, entry := range resp.Failed {
			failed = append(failed, failedEntry{ID: entry.Id, Code: entry.Code, Message: entry.Message, SenderFault: entry.SenderFault})
		}
		body, err := json.Marshal(struct {
			Successful []successfulEntry `json:"Successful"`
			Failed     []failedEntry     `json:"Failed"`
		}{Successful: successful, Failed: failed})
		if err != nil {
			http_server.Send(500, []byte(err.Error()))
			return
		}
		sendJSON(200, body)
	case "DeleteMessageBatch", "ChangeMessageVisibilityBatch":
		if sendBatchError(val, data) {
			return
		}
		resp := &pb.MessageReceiptBatchResponse{}
		if err := resp.UnmarshalVT(data); err != nil {
			http_server.Send(500, []byte(err.Error()))
			return
		}
		type successfulEntry struct {
			ID string `json:"Id"`
		}
		type failedEntry struct {
			ID          string `json:"Id"`
			Code        string `json:"Code"`
			Message     string `json:"Message"`
			SenderFault bool   `json:"SenderFault"`
		}
		successful := make([]successfulEntry, 0, len(resp.Successful))
		for _, entry := range resp.Successful {
			successful = append(successful, successfulEntry{ID: entry.Id})
		}
		failed := make([]failedEntry, 0, len(resp.Failed))
		for _, entry := range resp.Failed {
			failed = append(failed, failedEntry{ID: entry.Id, Code: entry.Code, Message: entry.Message, SenderFault: entry.SenderFault})
		}
		body, err := json.Marshal(struct {
			Successful []successfulEntry `json:"Successful"`
			Failed     []failedEntry     `json:"Failed"`
		}{Successful: successful, Failed: failed})
		if err != nil {
			http_server.Send(500, []byte(err.Error()))
			return
		}
		sendJSON(200, body)
	case "ReceiveMessage":
		if val != 200 {
			if string(data) == "Long polling is not supported" {
				sendUnsupported(string(data))
				return
			}
			http_server.Send(int(val), data)
			return
		}
		resp := &pb.ReceiveMessageResponse{}
		if err := resp.UnmarshalVT(data); err != nil {
			http_server.Send(500, []byte(err.Error()))
			return
		}
		messages := make([]*pb.ReceivedMessage, 0, len(resp.Messages))
		messages = append(messages, resp.Messages...)
		body, err := json.Marshal(struct {
			Messages []*pb.ReceivedMessage `json:"Messages"`
		}{Messages: messages})
		if err != nil {
			http_server.Send(500, []byte(err.Error()))
			return
		}
		sendJSON(200, body)
	case "DeleteMessage":
		if val != 200 {
			http_server.Send(int(val), data)
			return
		}
		sendJSON(200, []byte(`{}`))
	case "ChangeMessageVisibility":
		if val != 200 {
			http_server.Send(int(val), data)
			return
		}
		sendJSON(200, []byte(`{}`))
	default:
		panic("unknown asyncRecv name: " + string(name))
	}
}

func sendUnsupported(message string) {
	body, err := json.Marshal(struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}{Type: "com.amazonaws.sqs#UnsupportedOperation", Message: message})
	if err != nil {
		http_server.Send(500, []byte(err.Error()))
		return
	}
	sendJSON(400, body)
}

func sendBatchError(val uint64, data []byte) bool {
	if val == 200 {
		return false
	}
	if val == 400 {
		switch string(data) {
		case "EmptyBatchRequest", "TooManyEntriesInBatchRequest", "InvalidBatchEntryId", "BatchEntryIdsNotDistinct", "BatchRequestTooLong", "QueueDoesNotExist":
			body, err := json.Marshal(struct {
				Type    string `json:"__type"`
				Message string `json:"message"`
			}{Type: "com.amazonaws.sqs#" + string(data), Message: string(data)})
			if err != nil {
				http_server.Send(500, []byte(err.Error()))
				return true
			}
			sendJSON(400, body)
			return true
		}
	}
	http_server.Send(int(val), data)
	return true
}

func asyncName(action, host string) []byte {
	return []byte(action + "\x00" + host)
}

func queueURL(name, host string) string {
	if host == "" {
		host = "localhost"
	}
	return "http://" + host + "/000000000000/" + name
}

func queueNameFromURL(queueURL string) (string, bool) {
	path := queueURL
	if schemeEnd := strings.Index(path, "://"); schemeEnd >= 0 {
		path = path[schemeEnd+3:]
		pathStart := strings.IndexByte(path, '/')
		if pathStart < 0 {
			return "", false
		}
		path = path[pathStart:]
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 || parts[len(parts)-1] == "" {
		return "", false
	}
	return parts[len(parts)-1], true
}

func sendJSON(status int, body []byte) {
	http_server.Send(status, body, http_server.WithHeaders(map[string]string{
		"content-type": "application/x-amz-json-1.0",
	}))
}
