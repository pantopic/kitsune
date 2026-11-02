package main

import (
	"github.com/tidwall/gjson"

	"github.com/pantopic/ext-http-server/sdk-go"
	"github.com/pantopic/ext-shard-client/sdk-go"

	"github.com/pantopic/kitsune/module/pkg/query"
)

var (
	bodyBuf = make([]byte, 1<<20)
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
		headers := http_server.GetHeaders(
			"content-type",
		)
		if headers["content-type"] != "application/x-amz-json-1.0" {
			http_server.Send(400, []byte(`invalid content-type`))
			return
		}
		body := http_server.BodyRead(bodyBuf)
		action := gjson.GetBytes(body, "Action").String()
		switch action {
		case "ListQueues":
			// maxResults := gjson.GetBytes(body, "MaxResults").Int()
			// nextToken := gjson.GetBytes(body, "NextToken").String()
			// queueNamePrefix := gjson.GetBytes(body, "QueueNamePrefix").String()
			http_server.Send(200, []byte(`results`))
		default:
			http_server.Send(400, []byte(`invalid action`))
			return
		}
	case "/health":
		queueShard().ReadAsync([]byte(`health`), []byte{query.HEALTH_CHECK}, true)
	default:
		http_server.Send(404, []byte(`not found`))
	}
}

func asyncRecv(name, data []byte, val uint64, err error) {
	if err != nil {
		http_server.Send(500, []byte(err.Error()))
		return
	}
	switch string(name) {
	case "health":
		http_server.Send(int(val), data)
	default:
		panic("unknown asyncRecv name: " + string(name))
	}
}
