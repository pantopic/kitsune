package main

import (
	"errors"

	"github.com/aperturerobotics/protobuf-go-lite"

	"github.com/pantopic/ext-global/sdk-go"
)

type (
	Message protobuf_go_lite.Message
)

const (
	KV_FLAG_PATCH uint8 = 1 << iota
	KV_FLAG_COMPRESSED
)

const (
	CMD_INTERNAL_TERM byte = iota
	CMD_INTERNAL_TICK
	CMD_MESSAGE_SEND
)

const (
	QUERY_HEADER byte = iota
	QUERY_KV_RANGE
	QUERY_LEASE_LEASES
	QUERY_LEASE_TIME_TO_LIVE
	QUERY_WATCH_PROGRESS
)

const (
	WatchMessageType__UNKNOWN byte = iota
	WatchMessageType_CANCELED
	WatchMessageType_ERR_COMPACTED
	WatchMessageType_ERR_EXISTS
	WatchMessageType_EVENT_BATCH
	WatchMessageType_EVENT_SYNC
	WatchMessageType_INIT
	WatchMessageType_NOTIFY
)

var (
	// MAX_MESSAGE_SIZE is the maximum allowed size for messages.
	MAX_MESSAGE_SIZE = global.NewUint64(`MAX_MESSAGE_SIZE`, 1<<20)

	// MAX_POLL_WAIT_TIME is the maximum allowed wait time for messages (in seconds).
	MAX_POLL_WAIT_TIME = global.NewUint64(`MAX_POLL_WAIT_TIME`, 20)

	// MAX_DELAY_TIME is the maximum allowed delay time for messages (in seconds).
	MAX_DELAY_TIME = global.NewUint64(`MAX_DELAY_TIME`, 900)

	// MAX_IN_FLIGHT is the maximum allowed number of in-flight messages.
	MAX_IN_FLIGHT = global.NewUint64(`MAX_IN_FLIGHT`, 120_000)

	// MAX_QUEUE_NAME_LEN is the maximum allowed length for queue names.
	MAX_QUEUE_NAME_LEN = global.NewUint64(`MAX_QUEUE_NAME_LEN`, 80)

	// MAX_TAG_KEY_LEN is the maximum allowed length for tag keys.
	MAX_TAG_KEY_LEN = global.NewUint64(`MAX_TAG_KEY_LEN`, 128)

	// MAX_TAG_VAL_LEN is the maximum allowed length for tag values.
	MAX_TAG_VAL_LEN = global.NewUint64(`MAX_TAG_VAL_LEN`, 256)
)

var (
	ErrChecksumInvalid = errors.New(`Checksum invalid`)
	ErrChecksumMissing = errors.New(`Checksum missing`)
	ErrValueInvalid    = errors.New(`Value invalid`)
	ErrKeyInvalid      = errors.New(`Key invalid`)
	ErrKeyMissing      = errors.New(`Key missing`)
	ErrNotFound        = errors.New(`Not found`)
)
