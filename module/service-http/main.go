package main

import (
	"github.com/pantopic/ext-http-server/sdk-go"
)

func init() {
	http_server.Init(
		http_server.WithHandler(httpHandler),
	)
}

func main() {
}

func httpHandler(method, path []byte) {
	switch string(path) {
	case "/":
		http_server.Send(200, []byte(`pantopic_power_level 9001`))
	case "/health":
		http_server.Send(200, []byte(`{"health":"true","reason":""}`))
	case "/version":
		http_server.Send(200, []byte(`{"etcdserver":"3.5.25","etcdcluster":"3.5.0"}`))
	default:
		http_server.Send(404, []byte(`not found`))
	}
}
