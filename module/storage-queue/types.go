package main

import (
	"github.com/pantopic/ext-global/sdk-go"
)

var (
	ARN_PARTITION  = global.NewString(`ARN_PARTITION`, "aws")
	ARN_SERVICE    = global.NewString(`ARN_SERVICE`, "sqs")
	ARN_REGION     = global.NewString(`ARN_REGION`, "us-est-1")
	ARN_ACCOUNT_ID = global.NewString(`ARN_ACCOUNT_ID`, "123456789012")
)
