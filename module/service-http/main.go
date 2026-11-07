package main

import (
	"github.com/pantopic/ext-http-server/sdk-go"
	"github.com/pantopic/ext-shard-client/sdk-go"

	"github.com/pantopic/kitsune/module/pkg/pb"
	"github.com/pantopic/kitsune/module/pkg/query"
)

var (
	bodyBuf       = make([]byte, 1<<20)
	request       = &pb.Request{}
	reqListQueues = &pb.ListQueuesRequest{}
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
		if err := request.UnmarshalJSON(body); err != nil {
			http_server.Send(400, []byte(err.Error()))
			return
		}
		switch request.Action {
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
			if err := queueShard().ReadAsync([]byte(request.Action), append(b, query.QUEUE_LIST), false); err != nil {
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
	switch string(name) {
	case "health":
		if val == 0 {
			http_server.Send(500, []byte(`Invalid response code`))
			return
		}
		http_server.Send(int(val), data)
	case "ListQueues":
		http_server.Send(int(val), data)
	default:
		panic("unknown asyncRecv name: " + string(name))
	}
}
