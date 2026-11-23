package kitsune

import (
	_ "embed"

	"github.com/pantopic/cluster/iface"
)

const (
	BridgeCron   = "pantopic/kitsune/bridge/cron"
	ServiceHttp  = "pantopic/kitsune/service/http"
	StorageQueue = "pantopic/kitsune/storage/queue"
	ControlBasic = "pantopic/kitsune/control/basic"
	Version      = 0
)

func Register(c pantopic.ModuleRegistry) {
	c.RegisterBridge(BridgeCron, WasmBridgeCron)
	c.RegisterService(ServiceHttp, WasmServiceHttp)
	c.RegisterStorage(StorageQueue, WasmStorageQueue)
	c.RegisterControl(ControlBasic, WasmControlBasic)
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

//go:embed wasm/bridge-cron.wasm
var WasmBridgeCron []byte

//go:embed wasm/bridge-cron.dev.wasm
var WasmBridgeCronDev []byte

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
