package main

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
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

type messageAttributeJSON struct {
	DataType         string   `json:"DataType"`
	StringValue      *string  `json:"StringValue,omitempty"`
	BinaryValue      *[]byte  `json:"BinaryValue,omitempty"`
	StringListValues []string `json:"StringListValues,omitempty"`
	BinaryListValues []string `json:"BinaryListValues,omitempty"`
}

func parseMessageAttributes(input map[string]json.RawMessage) map[string]*pb.MessageAttribute {
	output := make(map[string]*pb.MessageAttribute, len(input))
	for name, raw := range input {
		var value struct {
			DataType         string   `json:"DataType"`
			StringValue      *string  `json:"StringValue"`
			BinaryValue      *string  `json:"BinaryValue"`
			StringListValues []string `json:"StringListValues"`
			BinaryListValues []string `json:"BinaryListValues"`
		}
		attr := &pb.MessageAttribute{}
		if err := json.Unmarshal(raw, &value); err != nil {
			attr.Invalid = true
			output[name] = attr
			continue
		}
		attr.DataType = value.DataType
		if value.StringValue != nil {
			attr.StringValueSet = true
			attr.StringValue = *value.StringValue
		}
		if value.BinaryValue != nil {
			attr.BinaryValueSet = true
			var err error
			attr.BinaryValue, err = base64.StdEncoding.DecodeString(*value.BinaryValue)
			if err != nil {
				attr.Invalid = true
			}
		}
		if len(value.StringListValues) > 0 || len(value.BinaryListValues) > 0 {
			attr.Invalid = true
		}
		output[name] = attr
	}
	return output
}

func messageAttributesJSON(input map[string]*pb.MessageAttribute) map[string]messageAttributeJSON {
	if len(input) == 0 {
		return nil
	}
	output := make(map[string]messageAttributeJSON, len(input))
	for name, attr := range input {
		value := messageAttributeJSON{DataType: attr.DataType}
		if attr.StringValueSet {
			value.StringValue = &attr.StringValue
		}
		if attr.BinaryValueSet {
			value.BinaryValue = &attr.BinaryValue
		}
		output[name] = value
	}
	return output
}

type receiveMessageRequest struct {
	QueueURL                    string   `json:"QueueUrl"`
	MaxNumberOfMessages         *uint64  `json:"MaxNumberOfMessages"`
	VisibilityTimeout           *uint64  `json:"VisibilityTimeout"`
	WaitTimeSeconds             *uint64  `json:"WaitTimeSeconds"`
	MessageAttributeNames       []string `json:"MessageAttributeNames"`
	MessageSystemAttributeNames []string `json:"MessageSystemAttributeNames"`
	AttributeNames              []string `json:"AttributeNames"`
	ReceiveRequestAttemptID     string   `json:"ReceiveRequestAttemptId"`
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
	QueueURL          string  `json:"QueueUrl"`
	ReceiptHandle     string  `json:"ReceiptHandle"`
	VisibilityTimeout *uint64 `json:"VisibilityTimeout"`
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
			sendSQSError(400, "InvalidParameterValue", "invalid content-type")
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
			sendSQSError(400, "InvalidParameterValue", err.Error())
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
				sendSQSError(400, "InvalidParameterValue", err.Error())
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
				sendSQSError(400, "InvalidParameterValue", err.Error())
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
				sendSQSError(400, "InvalidParameterValue", err.Error())
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
				sendSQSError(400, "InvalidParameterValue", err.Error())
				return
			}
			name, ok := queueNameFromURL(req.QueueURL)
			if !ok {
				sendSQSError(400, "InvalidAddress", "invalid QueueUrl")
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
				sendSQSError(400, "InvalidParameterValue", err.Error())
				return
			}
			name, ok := queueNameFromURL(req.QueueURL)
			if !ok {
				sendSQSError(400, "InvalidAddress", "invalid QueueUrl")
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
				sendSQSError(400, "InvalidParameterValue", err.Error())
				return
			}
			name, ok := queueNameFromURL(req.QueueURL)
			if !ok {
				sendSQSError(400, "InvalidAddress", "invalid QueueUrl")
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
				sendSQSError(400, "InvalidParameterValue", err.Error())
				return
			}
			name, ok := queueNameFromURL(req.QueueURL)
			if !ok {
				sendSQSError(400, "InvalidAddress", "invalid QueueUrl")
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
				sendSQSError(400, "InvalidParameterValue", err.Error())
				return
			}
			if req.MessageDeduplicationID != "" || req.MessageGroupID != "" {
				sendUnsupported("FIFO fields are not supported")
				return
			}
			name, ok := queueNameFromURL(req.QueueURL)
			if !ok {
				sendSQSError(400, "InvalidAddress", "invalid QueueUrl")
				return
			}
			sendReq := &pb.SendMessageRequest{QueueName: name, MessageBody: req.MessageBody,
				MessageAttributes: parseMessageAttributes(req.MessageAttributes), MessageSystemAttributes: parseMessageAttributes(req.MessageSystemAttributes)}
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
				sendSQSError(400, "InvalidParameterValue", err.Error())
				return
			}
			name, ok := queueNameFromURL(req.QueueURL)
			if !ok {
				sendSQSError(400, "InvalidAddress", "invalid QueueUrl")
				return
			}
			batch := &pb.SendMessageBatchRequest{QueueName: name}
			for _, entry := range req.Entries {
				item := &pb.SendMessageBatchRequestEntry{
					Id:                      entry.ID,
					MessageBodySet:          entry.MessageBody != nil,
					DelaySecondsSet:         entry.DelaySeconds != nil,
					UnsupportedFields:       entry.MessageDeduplicationID != "" || entry.MessageGroupID != "",
					MessageAttributes:       parseMessageAttributes(entry.MessageAttributes),
					MessageSystemAttributes: parseMessageAttributes(entry.MessageSystemAttributes),
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
				sendSQSError(400, "InvalidParameterValue", err.Error())
				return
			}
			if req.ReceiveRequestAttemptID != "" {
				sendUnsupported("FIFO fields are not supported")
				return
			}
			systemNames := append(req.MessageSystemAttributeNames, req.AttributeNames...)
			for _, name := range systemNames {
				if name != "AWSTraceHeader" && name != "ApproximateReceiveCount" {
					sendUnsupported("Requested message system attribute is not supported")
					return
				}
			}
			name, ok := queueNameFromURL(req.QueueURL)
			if !ok {
				sendSQSError(400, "InvalidAddress", "invalid QueueUrl")
				return
			}
			var visibilityTimeout uint64
			if req.VisibilityTimeout != nil {
				visibilityTimeout = *req.VisibilityTimeout
			}
			receiveReq := &pb.ReceiveMessageRequest{
				QueueName:                   name,
				MaxNumberOfMessagesSet:      req.MaxNumberOfMessages != nil,
				VisibilityTimeout:           visibilityTimeout,
				VisibilityTimeoutSet:        req.VisibilityTimeout != nil,
				WaitTimeSecondsSet:          req.WaitTimeSeconds != nil,
				MessageAttributeNames:       req.MessageAttributeNames,
				MessageSystemAttributeNames: systemNames,
			}
			if req.MaxNumberOfMessages != nil {
				receiveReq.MaxNumberOfMessages = *req.MaxNumberOfMessages
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
				sendSQSError(400, "InvalidParameterValue", err.Error())
				return
			}
			name, ok := queueNameFromURL(req.QueueURL)
			if !ok {
				sendSQSError(400, "InvalidAddress", "invalid QueueUrl")
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
				sendSQSError(400, "InvalidParameterValue", err.Error())
				return
			}
			name, ok := queueNameFromURL(req.QueueURL)
			if !ok {
				sendSQSError(400, "InvalidAddress", "invalid QueueUrl")
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
				sendSQSError(400, "InvalidParameterValue", err.Error())
				return
			}
			if req.VisibilityTimeout == nil {
				sendSQSError(400, "InvalidParameterValue", "VisibilityTimeout is required")
				return
			}
			name, ok := queueNameFromURL(req.QueueURL)
			if !ok {
				sendSQSError(400, "InvalidAddress", "invalid QueueUrl")
				return
			}
			b, err := (&pb.ChangeMessageVisibilityRequest{
				QueueName:         name,
				ReceiptHandle:     req.ReceiptHandle,
				VisibilityTimeout: *req.VisibilityTimeout,
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
			sendUnsupported("invalid action")
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
	if action != "health" && val != 200 {
		sendOperationError(action, val, data)
		return
	}
	switch action {
	case "health":
		if val == 0 {
			http_server.Send(500, []byte(`Invalid response code`))
			return
		}
		http_server.Send(int(val), data)
	case "ListQueues":
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
		sendJSON(200, []byte(`{}`))
	case "PurgeQueue":
		sendJSON(200, []byte(`{}`))
	case "GetQueueAttributes":
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
		sendJSON(200, []byte(`{}`))
	case "SendMessage":
		resp := &pb.SendMessageResponse{}
		if err := resp.UnmarshalVT(data); err != nil {
			http_server.Send(500, []byte(err.Error()))
			return
		}
		body, err := json.Marshal(struct {
			MessageID                    string `json:"MessageId"`
			Md5OfMessageBody             string `json:"MD5OfMessageBody"`
			Md5OfMessageAttributes       string `json:"MD5OfMessageAttributes,omitempty"`
			Md5OfMessageSystemAttributes string `json:"MD5OfMessageSystemAttributes,omitempty"`
		}{MessageID: resp.MessageId, Md5OfMessageBody: resp.Md5OfMessageBody, Md5OfMessageAttributes: resp.Md5OfMessageAttributes, Md5OfMessageSystemAttributes: resp.Md5OfMessageSystemAttributes})
		if err != nil {
			http_server.Send(500, []byte(err.Error()))
			return
		}
		sendJSON(200, body)
	case "SendMessageBatch":
		resp := &pb.SendMessageBatchResponse{}
		if err := resp.UnmarshalVT(data); err != nil {
			http_server.Send(500, []byte(err.Error()))
			return
		}
		type successfulEntry struct {
			ID                           string `json:"Id"`
			MessageID                    string `json:"MessageId"`
			Md5OfMessageBody             string `json:"MD5OfMessageBody"`
			Md5OfMessageAttributes       string `json:"MD5OfMessageAttributes,omitempty"`
			Md5OfMessageSystemAttributes string `json:"MD5OfMessageSystemAttributes,omitempty"`
		}
		type failedEntry struct {
			ID          string `json:"Id"`
			Code        string `json:"Code"`
			Message     string `json:"Message"`
			SenderFault bool   `json:"SenderFault"`
		}
		successful := make([]successfulEntry, 0, len(resp.Successful))
		for _, entry := range resp.Successful {
			successful = append(successful, successfulEntry{ID: entry.Id, MessageID: entry.MessageId, Md5OfMessageBody: entry.Md5OfMessageBody, Md5OfMessageAttributes: entry.Md5OfMessageAttributes, Md5OfMessageSystemAttributes: entry.Md5OfMessageSystemAttributes})
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
		resp := &pb.ReceiveMessageResponse{}
		if err := resp.UnmarshalVT(data); err != nil {
			http_server.Send(500, []byte(err.Error()))
			return
		}
		type message struct {
			MessageID              string                          `json:"MessageId"`
			ReceiptHandle          string                          `json:"ReceiptHandle"`
			Body                   string                          `json:"Body"`
			Md5OfBody              string                          `json:"MD5OfBody"`
			MessageAttributes      map[string]messageAttributeJSON `json:"MessageAttributes,omitempty"`
			Md5OfMessageAttributes string                          `json:"MD5OfMessageAttributes,omitempty"`
			Attributes             map[string]string               `json:"Attributes,omitempty"`
		}
		messages := make([]message, 0, len(resp.Messages))
		for _, item := range resp.Messages {
			digest := md5.Sum([]byte(item.Body))
			messages = append(messages, message{MessageID: item.MessageId, ReceiptHandle: item.ReceiptHandle, Body: item.Body,
				Md5OfBody: hex.EncodeToString(digest[:]), MessageAttributes: messageAttributesJSON(item.MessageAttributes), Md5OfMessageAttributes: item.Md5OfMessageAttributes, Attributes: item.Attributes})
		}
		body, err := json.Marshal(struct {
			Messages []message `json:"Messages"`
		}{Messages: messages})
		if err != nil {
			http_server.Send(500, []byte(err.Error()))
			return
		}
		sendJSON(200, body)
	case "DeleteMessage":
		sendJSON(200, []byte(`{}`))
	case "ChangeMessageVisibility":
		sendJSON(200, []byte(`{}`))
	default:
		panic("unknown asyncRecv name: " + string(name))
	}
}

func sendUnsupported(message string) {
	sendSQSError(400, "UnsupportedOperation", message)
}

func sendSQSError(status int, code, message string) {
	body, err := json.Marshal(struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
	}{Type: "com.amazonaws.sqs#" + code, Message: message})
	if err != nil {
		http_server.Send(500, []byte(err.Error()))
		return
	}
	sendJSON(status, body)
}

func sendOperationError(action string, val uint64, data []byte) {
	message := string(data)
	if val == 400 {
		var code string
		switch message {
		case "EmptyBatchRequest", "TooManyEntriesInBatchRequest", "InvalidBatchEntryId", "BatchEntryIdsNotDistinct", "BatchRequestTooLong", "QueueDoesNotExist":
			code = message
		case "Queue does not exist":
			code = "QueueDoesNotExist"
		case "Queue name exists with different attributes":
			code = "QueueNameExists"
		case "Invalid attribute name":
			code = "InvalidAttributeName"
		case "Invalid attribute value":
			code = "InvalidAttributeValue"
			if action == "ReceiveMessage" || action == "ChangeMessageVisibility" {
				code = "InvalidParameterValue"
			}
		case "Receipt handle is invalid":
			code = "ReceiptHandleIsInvalid"
		case "Message body invalid":
			code = "InvalidMessageContents"
		case "Long polling is not supported":
			code = "UnsupportedOperation"
		case "Queue name required", "Queue name too long", "Queue name invalid", "Message body required", "Message body too large", "Message delay invalid", "Message attributes are invalid", "Invalid MaxNumberOfMessages", "Invalid MaxResults", "Invalid NextToken":
			code = "InvalidParameterValue"
		}
		if code != "" {
			sendSQSError(400, code, message)
			return
		}
	}
	sendSQSError(500, "InternalError", message)
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
