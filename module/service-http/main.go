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

func httpHandler(method, path []byte) (code int, res []byte) {
	switch string(path) {
	case "/metrics":
		res = append(res, []byte(`pantopic_power_level 9001`)...)
	case "/health":
		res = append(res, []byte(`{"health":"true","reason":""}`)...)
	case "/version":
		res = append(res, []byte(`{"etcdserver":"3.5.25","etcdcluster":"3.5.0"}`)...)
	default:
		code = 405
	}
	return
}
