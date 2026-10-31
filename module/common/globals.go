package common

import (
	"github.com/aperturerobotics/protobuf-go-lite"

	"github.com/pantopic/ext-global/sdk-go"
)

type (
	Message protobuf_go_lite.Message
)

const (
	CMD_INTERNAL_TERM byte = iota
	CMD_INTERNAL_TICK
	CMD_QUEUE_CREATE
	CMD_QUEUE_DELETE
	CMD_QUEUE_PURGE
	CMD_MESSAGE_SEND
	CMD_MESSAGE_DELETE
	CMD_MESSAGE_RECEIVE
	CMD_MESSAGE_REFRESH
)

const (
	QUERY_QUEUE_LIST byte = iota
	QUERY_QUEUE_URL
	QUERY_QUEUE_ATTRIBUTES
	QUERY_QUEUE_DEAD_LETTER_SOURCE
)

var (
	// ALLOW_UNSAFE_GET determines whether unsafe get operations are allowed.
	ALLOW_UNSAFE_GET = global.NewBool(`ALLOW_UNSAFE_GET`, false)
)

var (
	// MAX_MESSAGE_SIZE is the maximum allowed size for messages.
	MAX_MESSAGE_SIZE = global.NewUint64(`MAX_MESSAGE_SIZE`, 1<<20)

	// MAX_MESSAGE_ID_LEN is the maximum allowed length for message IDs.
	MAX_MESSAGE_ID_LEN = global.NewUint64(`MAX_MESSAGE_ID_LEN`, 80)

	// MAX_MESSAGE_ATTRIBUTES is the maximum allowed number of message attributes.
	MAX_MESSAGE_ATTRIBUTES = global.NewUint64(`MAX_MESSAGE_ATTRIBUTES`, 10)

	// MAX_MESSAGE_BATCH_SIZE is the maximum allowed number of messages in a batch.
	MAX_MESSAGE_BATCH_SIZE = global.NewUint64(`MAX_MESSAGE_BATCH_SIZE`, 10)

	// MAX_MESSAGE_GROUP_ID_LEN is the maximum allowed length for message group IDs.
	MAX_MESSAGE_GROUP_ID_LEN = global.NewUint64(`MAX_MESSAGE_GROUP_ID_LEN`, 128)

	// MAX_MESSAGE_RETENTION is the maximum allowed retention time for messages (in seconds).
	MAX_MESSAGE_RETENTION = global.NewUint64(`MAX_MESSAGE_RETENTION`, 86400*14)

	// DEFAULT_MESSAGE_RETENTION is the default retention time for messages (in seconds).
	DEFAULT_MESSAGE_RETENTION = global.NewUint64(`DEFAULT_MESSAGE_RETENTION`, 86400*4)

	// DEFAULT_MESSAGE_VISIBILITY is the default visibility timeout for messages (in seconds).
	DEFAULT_MESSAGE_VISIBILITY = global.NewUint64(`DEFAULT_MESSAGE_VISIBILITY`, 30)

	// MAX_MESSAGE_VISIBILITY is the maximum allowed visibility timeout for messages (in seconds).
	MAX_MESSAGE_VISIBILITY = global.NewUint64(`MAX_MESSAGE_VISIBILITY`, 3600*12)

	// MAX_FIFO_ACTION_RATE is the maximum allowed rate for FIFO actions (per action per second).
	MAX_FIFO_ACTION_RATE = global.NewUint64(`MAX_FIFO_ACTION_RATE`, 300)

	// MAX_FIFO_MESSAGE_RATE is the maximum allowed rate for FIFO messages (per action per second).
	MAX_FIFO_MESSAGE_RATE = global.NewUint64(`MAX_FIFO_MESSAGE_RATE`, 3000)

	// MAX_POLL_WAIT_TIME is the maximum allowed wait time for messages (in seconds).
	MAX_POLL_WAIT_TIME = global.NewUint64(`MAX_POLL_WAIT_TIME`, 20)

	// MAX_DELAY_TIME is the maximum allowed delay time for messages (in seconds).
	MAX_DELAY_TIME = global.NewUint64(`MAX_DELAY_TIME`, 60*15)

	// MAX_IN_FLIGHT is the maximum allowed number of in-flight messages.
	MAX_IN_FLIGHT = global.NewUint64(`MAX_IN_FLIGHT`, 120_000)

	// MAX_QUEUE_NAME_LEN is the maximum allowed length for queue names.
	MAX_QUEUE_NAME_LEN = global.NewUint64(`MAX_QUEUE_NAME_LEN`, 80)

	// MAX_TAG_KEY_LEN is the maximum allowed length for tag keys.
	MAX_TAG_KEY_LEN = global.NewUint64(`MAX_TAG_KEY_LEN`, 128)

	// MAX_TAG_VAL_LEN is the maximum allowed length for tag values.
	MAX_TAG_VAL_LEN = global.NewUint64(`MAX_TAG_VAL_LEN`, 256)
)
