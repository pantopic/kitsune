package kitsune

import (
	_ "embed"
)

const (
	NameServiceHttp  = "pantopic/kitsune/service/http"
	NameStorageQueue = "pantopic/kitsune/storage/queue"
	NameControlBasic = "pantopic/kitsune/control/basic"
	Version          = 0
)

func Register(runtimeService, runtimeStorage, runtimeControl interface {
	Register(name string, wasm []byte)
}) {
	runtimeService.Register(NameServiceHttp, WasmServiceHttp)
	runtimeStorage.Register(NameStorageQueue, WasmStorageQueue)
	runtimeControl.Register(NameControlBasic, WasmControlBasic)
}

//go:embed service-http.wasm
var WasmServiceHttp []byte

//go:embed service-http.dev.wasm
var WasmServiceHttpDev []byte

//go:embed storage-queue.wasm
var WasmStorageQueue []byte

//go:embed storage-queue.dev.wasm
var WasmStorageQueueDev []byte

//go:embed control-basic.wasm
var WasmControlBasic []byte

//go:embed control-basic.dev.wasm
var WasmControlBasicDev []byte

/*
//go:embed service-http.zig.wasm
var ServiceHttpZigWasm []byte

//go:embed service-http.zig.dev.wasm
var ServiceHttpZigDevWasm []byte

//go:embed storage-queue.zig.wasm
var StorageQueueZigWasm []byte

//go:embed storage-queue.zig.dev.wasm
var StorageQueueZigDevWasm []byte
*/
