package kitsune

import (
	_ "embed"
)

const (
	ServiceHttpName  = "pantopic/kitsune/service/http"
	StorageQueueName = "pantopic/kitsune/storage/queue"
	Version          = 0
)

//go:embed service-http.wasm
var ServiceHttpWasm []byte

//go:embed service-http.dev.wasm
var ServiceHttpDevWasm []byte

//go:embed storage-queue.wasm
var StorageQueueWasm []byte

//go:embed storage-queue.dev.wasm
var StorageQueueDevWasm []byte

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
