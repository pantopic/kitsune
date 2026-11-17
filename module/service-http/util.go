package main

import (
	"github.com/pantopic/ext-shard-client/sdk-go"
)

var (
	bankName       = []byte(`default`)
	shardNameQueue = []byte(`queue`)
)

func queueShard() shard_client.Client {
	return shard_client.New(bankName, shardNameQueue)
}
