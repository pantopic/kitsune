package main

import (
	"errors"
)

var (
	ErrChecksumInvalid            = errors.New(`Checksum invalid`)
	ErrChecksumMissing            = errors.New(`Checksum missing`)
	ErrValueInvalid               = errors.New(`Value invalid`)
	ErrKeyInvalid                 = errors.New(`Key invalid`)
	ErrKeyMissing                 = errors.New(`Key missing`)
	ErrNotFound                   = errors.New(`Not found`)
	ErrTermExpired                = errors.New(`Term expired`)
	ErrQueueNameRequired          = errors.New(`Queue name required`)
	ErrQueueNameTooLong           = errors.New(`Queue name too long`)
	ErrQueueNameInvalid           = errors.New(`Queue name invalid`)
	ErrQueueDoesNotExist          = errors.New(`Queue does not exist`)
	ErrQueueNameExists            = errors.New(`Queue name exists with different attributes`)
	ErrQueueAttributeNameInvalid  = errors.New(`Invalid attribute name`)
	ErrQueueAttributeValueInvalid = errors.New(`Invalid attribute value`)
	ErrGroupNameRequired          = errors.New(`Group name required`)
	ErrGroupNameTooLong           = errors.New(`Group name too long`)
)
