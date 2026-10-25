package main

import (
	"errors"
)

var (
	ErrChecksumInvalid   = errors.New(`Checksum invalid`)
	ErrChecksumMissing   = errors.New(`Checksum missing`)
	ErrValueInvalid      = errors.New(`Value invalid`)
	ErrKeyInvalid        = errors.New(`Key invalid`)
	ErrKeyMissing        = errors.New(`Key missing`)
	ErrNotFound          = errors.New(`Not found`)
	ErrTermExpired       = errors.New(`Term expired`)
	ErrQueueNameRequired = errors.New(`Queue name required`)
	ErrQueueNameTooLong  = errors.New(`Queue name too long`)
	ErrGroupNameRequired = errors.New(`Group name required`)
	ErrGroupNameTooLong  = errors.New(`Group name too long`)
)
