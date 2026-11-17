package kitsune

import (
	_ "embed"

	"github.com/pantopic/cluster/iface"
)

const (
	NameServiceHttp  = "pantopic/kitsune/service/http"
	NameStorageQueue = "pantopic/kitsune/storage/queue"
	NameControlBasic = "pantopic/kitsune/control/basic"
	Version          = 0
)

func Register(c pantopic.ModuleRegistry) {
	c.RegisterService(NameServiceHttp, WasmServiceHttp)
	c.RegisterStorage(NameStorageQueue, WasmStorageQueue)
	c.RegisterControl(NameControlBasic, WasmControlBasic)
}

//go:embed wasm/service-http.wasm
var WasmServiceHttp []byte

//go:embed wasm/service-http.dev.wasm
var WasmServiceHttpDev []byte

//go:embed wasm/storage-queue.wasm
var WasmStorageQueue []byte

//go:embed wasm/storage-queue.dev.wasm
var WasmStorageQueueDev []byte

//go:embed wasm/control-basic.wasm
var WasmControlBasic []byte

//go:embed wasm/control-basic.dev.wasm
var WasmControlBasicDev []byte

/*
//go:embed wasm/service-http.zig.wasm
var ServiceHttpZigWasm []byte

//go:embed wasm/service-http.zig.dev.wasm
var ServiceHttpZigDevWasm []byte

//go:embed wasm/storage-queue.zig.wasm
var StorageQueueZigWasm []byte

//go:embed wasm/storage-queue.zig.dev.wasm
var StorageQueueZigDevWasm []byte
*/
