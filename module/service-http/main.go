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
	reqListQueues  = &pb.ListQueuesRequest{}
	reqCreateQueue = &pb.CreateQueueRequest{}
)

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
		body := http_server.BodyRead(bodyBuf)
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
			if err := reqListQueues.UnmarshalJSON(body); err != nil {
				http_server.Send(400, []byte(err.Error()))
				return
			}
			b, err := reqListQueues.MarshalVT()
			if err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
			if err := queueShard().ReadAsync(asyncName(action, requestHost), append(b, query.QUEUE_LIST), false); err != nil {
				http_server.Send(500, []byte(err.Error()))
				return
			}
		case "CreateQueue":
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
		}{QueueURLs: urls})
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
	default:
		panic("unknown asyncRecv name: " + string(name))
	}
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

func sendJSON(status int, body []byte) {
	http_server.Send(status, body, http_server.WithHeaders(map[string]string{
		"content-type": "application/x-amz-json-1.0",
	}))
}
