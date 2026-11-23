package main

import (
	"time"

	"github.com/pantopic/ext-atomic/sdk-go"
	"github.com/pantopic/ext-bridge/sdk-go"
	"github.com/pantopic/ext-cron/sdk-go"
	"github.com/pantopic/ext-shard-client/sdk-go"

	"github.com/pantopic/kitsune/module/pkg/cmd"
	internal "github.com/pantopic/kitsune/module/pkg/pb"
)

const (
	TERM_CACHE = iota
)

func init() {
	bridge.Init(start, reload, stop)
	cron.Init(callback)
}

var (
	componentName  = []byte(`default`)
	shardNameQueue = []byte(`queue`)
	name           = []byte(`cron`)

	reqTerm = &internal.TermRequest{}
	reqTick = &internal.TickRequest{}

	termCache = atomic.NewUint64Set(0).Find(TERM_CACHE)
)

func main() {}

func start(term uint64, shard *bridge.Shard) {
	defer termCache.Store(term)
	setTerm(term)
	cron.Start(name, time.Second)
}

func reload(term uint64, shard *bridge.Shard) {
	defer termCache.Store(term)
	setTerm(term)
}

func stop(term uint64, shard *bridge.Shard) {
	defer termCache.Store(0)
	cron.Stop(name)
}

func callback(name []byte) {
	term := termCache.Load()
	if term == 0 {
		return
	}
	sendTick(term)
}

func setTerm(term uint64) {
	defer termCache.Store(term)
	reqTerm.Reset()
	reqTerm.Term = term
	data, err := reqTerm.MarshalVT()
	if err != nil {
		panic(`Unable to marshal term request: ` + err.Error())
	}
	if _, _, err = shardQueue().Apply(append(data, cmd.INTERNAL_TERM)); err != nil {
		panic("Error setting term: " + err.Error())
	}
}

func sendTick(term uint64) {
	reqTick.Reset()
	reqTick.Term = term
	data, err := reqTick.MarshalVT()
	if err != nil {
		panic(`Unable to marshal tick request: ` + err.Error())
	}
	if _, _, err = shardQueue().Apply(append(data, cmd.INTERNAL_TICK)); err != nil {
		panic("Error sending tick: " + err.Error())
	}
}

func shardQueue() shard_client.Client {
	return shard_client.New(componentName, shardNameQueue)
}
