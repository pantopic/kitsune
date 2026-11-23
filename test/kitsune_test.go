package kitsune_test

import (
	"bytes"
	"context"
	"crypto/md5"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pantopic/kitsune/embed"

	"github.com/pantopic/cluster"
	"github.com/pantopic/cluster/pb"
)

func TestAll(t *testing.T) {
	c, addr := setup(t)
	if c == nil {
		t.Fatal("setup failed")
	}

	queueURL := "http://" + addr + "/000000000000/kitsune-test-queue"
	var messageID string
	var delayedMessageID string
	var zeroVisibilityMessageID string
	var receiptHandle string
	var staleReceiptHandle string
	var earlierReceiptHandle string

	t.Run(`content-type`, func(t *testing.T) {
		var res *http.Response
		var err error
		timeout(t, time.Second, func() {
			res, err = c.Post(fmt.Sprintf("http://%s/", addr), "application/json", nil)
		})
		require.Nil(t, err, err)
		require.NotNil(t, res)
		require.Equal(t, res.StatusCode, 400)
		body, err := io.ReadAll(res.Body)
		require.Nil(t, err, err)
		assert.Equal(t, `invalid content-type`, string(body))
	})
	t.Run(`CreateQueue`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "CreateQueue", `{"QueueName":"kitsune-test-queue"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		assert.Equal(t, "application/x-amz-json-1.0", res.Header.Get("Content-Type"))
		assert.Contains(t, string(body), `"QueueUrl":"http://`+addr)
		assert.Contains(t, string(body), `/kitsune-test-queue"`)
	})
	t.Run(`ListQueues`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "ListQueues", `{"QueueNamePrefix":"kitsune-test"}`)
		require.Equal(t, 200, res.StatusCode, "%s", string(body))
		assert.Equal(t, "application/x-amz-json-1.0", res.Header.Get("Content-Type"))
		assert.Contains(t, string(body), `"QueueUrls":["http://`+addr)
		assert.Contains(t, string(body), `/kitsune-test-queue"]`)
	})
	t.Run(`GetQueueUrl`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "GetQueueUrl", `{"QueueName":"kitsune-test-queue"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		assert.Contains(t, string(body), `"QueueUrl":"http://`+addr)
		assert.Contains(t, string(body), `/kitsune-test-queue"`)
	})
	t.Run(`GetQueueAttributes`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "GetQueueAttributes", `{"QueueUrl":"`+queueURL+`","AttributeNames":["DelaySeconds","VisibilityTimeout","MaximumMessageSize"]}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		var attributesResponse struct {
			Attributes map[string]string `json:"Attributes"`
		}
		require.NoError(t, json.Unmarshal(body, &attributesResponse))
		assert.Equal(t, "0", attributesResponse.Attributes["DelaySeconds"])
		assert.Equal(t, "30", attributesResponse.Attributes["VisibilityTimeout"])
		assert.Equal(t, "1048576", attributesResponse.Attributes["MaximumMessageSize"])
	})
	t.Run(`SetQueueAttributes`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "SetQueueAttributes", `{"QueueUrl":"`+queueURL+`","Attributes":{"DelaySeconds":"10","VisibilityTimeout":"60"}}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		assert.Equal(t, `{}`, string(body))

		res, body = requestSQS(t, c, addr, "SetQueueAttributes", `{"QueueUrl":"`+queueURL+`","Attributes":{"DelaySeconds":"901"}}`)
		require.Equal(t, http.StatusBadRequest, res.StatusCode, string(body))
		assert.Equal(t, `Invalid attribute value`, string(body))
	})
	t.Run(`GetQueueAttributesAfterUpdate`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "GetQueueAttributes", `{"QueueUrl":"`+queueURL+`","AttributeNames":["DelaySeconds","VisibilityTimeout"]}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		var attributesResponse struct {
			Attributes map[string]string `json:"Attributes"`
		}
		require.NoError(t, json.Unmarshal(body, &attributesResponse))
		assert.Equal(t, "10", attributesResponse.Attributes["DelaySeconds"])
		assert.Equal(t, "60", attributesResponse.Attributes["VisibilityTimeout"])
	})
	t.Run(`ResetQueueDelay`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "SetQueueAttributes", `{"QueueUrl":"`+queueURL+`","Attributes":{"DelaySeconds":"0"}}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
	})
	t.Run(`SendMessage`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"hello kitsune"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		var sendResponse struct {
			MessageID        string `json:"MessageId"`
			Md5OfMessageBody string `json:"MD5OfMessageBody"`
		}
		require.NoError(t, json.Unmarshal(body, &sendResponse))
		require.NotEmpty(t, sendResponse.MessageID)
		require.Len(t, sendResponse.Md5OfMessageBody, 32)
		messageID = sendResponse.MessageID
	})
	t.Run(`ReceiveMessage`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		var receiveResponse struct {
			Messages []struct {
				MessageID     string `json:"MessageId"`
				ReceiptHandle string `json:"ReceiptHandle"`
				Body          string `json:"Body"`
			} `json:"Messages"`
		}
		require.NoError(t, json.Unmarshal(body, &receiveResponse))
		require.Len(t, receiveResponse.Messages, 1)
		assert.Equal(t, messageID, receiveResponse.Messages[0].MessageID)
		assert.Equal(t, "hello kitsune", receiveResponse.Messages[0].Body)
		require.NotEmpty(t, receiveResponse.Messages[0].ReceiptHandle)
		receiptHandle = receiveResponse.Messages[0].ReceiptHandle
		staleReceiptHandle = receiptHandle
	})
	t.Run(`ReceiveInvisibleMessage`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		assert.Equal(t, `{"Messages":[]}`, string(body))
	})
	t.Run(`ChangeMessageVisibility`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "ChangeMessageVisibility", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+receiptHandle+`","VisibilityTimeout":0}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		assert.Equal(t, `{}`, string(body))
	})
	t.Run(`ReceiveVisibleMessage`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		var receiveResponse struct {
			Messages []struct {
				MessageID     string `json:"MessageId"`
				ReceiptHandle string `json:"ReceiptHandle"`
			} `json:"Messages"`
		}
		require.NoError(t, json.Unmarshal(body, &receiveResponse))
		require.Len(t, receiveResponse.Messages, 1)
		assert.Equal(t, messageID, receiveResponse.Messages[0].MessageID)
		assert.NotEqual(t, staleReceiptHandle, receiveResponse.Messages[0].ReceiptHandle)
		receiptHandle = receiveResponse.Messages[0].ReceiptHandle
	})
	t.Run(`DeleteMessageWithStaleReceipt`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "DeleteMessage", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+staleReceiptHandle+`"}`)
		require.Equal(t, http.StatusBadRequest, res.StatusCode, string(body))
		assert.Equal(t, `Receipt handle is invalid`, string(body))
	})
	t.Run(`ChangeMessageVisibilityWithStaleReceipt`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "ChangeMessageVisibility", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+staleReceiptHandle+`","VisibilityTimeout":0}`)
		require.Equal(t, http.StatusBadRequest, res.StatusCode, string(body))
		assert.Equal(t, `Receipt handle is invalid`, string(body))
	})
	t.Run(`ChangeMessageVisibilityTimeout`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "ChangeMessageVisibility", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+receiptHandle+`","VisibilityTimeout":2}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		assert.Equal(t, `{}`, string(body))
	})
	t.Run(`ChangeMessageVisibilityAgain`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "ChangeMessageVisibility", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+receiptHandle+`","VisibilityTimeout":2}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		assert.Equal(t, `{}`, string(body))
	})
	t.Run(`ReceiveMessageBeforeVisibilityTimeout`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		assert.Equal(t, `{"Messages":[]}`, string(body))
	})
	t.Run(`ReceiveMessageAfterVisibilityTimeout`, func(t *testing.T) {
		time.Sleep(3 * time.Second)
		res, body := requestSQS(t, c, addr, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		var receiveResponse struct {
			Messages []struct {
				MessageID     string `json:"MessageId"`
				ReceiptHandle string `json:"ReceiptHandle"`
			} `json:"Messages"`
		}
		require.NoError(t, json.Unmarshal(body, &receiveResponse))
		require.Len(t, receiveResponse.Messages, 1)
		assert.Equal(t, messageID, receiveResponse.Messages[0].MessageID)
		receiptHandle = receiveResponse.Messages[0].ReceiptHandle
	})
	t.Run(`DeleteMessage`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "DeleteMessage", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+receiptHandle+`"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		assert.Equal(t, `{}`, string(body))
	})
	t.Run(`SendMessageForZeroVisibility`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"zero visibility"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		var sendResponse struct {
			MessageID string `json:"MessageId"`
		}
		require.NoError(t, json.Unmarshal(body, &sendResponse))
		require.NotEmpty(t, sendResponse.MessageID)
		zeroVisibilityMessageID = sendResponse.MessageID
	})
	t.Run(`ReceiveMessageWithZeroVisibility`, func(t *testing.T) {
		seen := map[string]string{}
		sameDeadline := false
		for i := 0; i < 10; i++ {
			res, body := requestSQS(t, c, addr, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","VisibilityTimeout":0}`)
			require.Equal(t, http.StatusOK, res.StatusCode, string(body))
			var receiveResponse struct {
				Messages []struct {
					MessageID     string `json:"MessageId"`
					ReceiptHandle string `json:"ReceiptHandle"`
					Body          string `json:"Body"`
				} `json:"Messages"`
			}
			require.NoError(t, json.Unmarshal(body, &receiveResponse))
			require.Len(t, receiveResponse.Messages, 1)
			assert.Equal(t, zeroVisibilityMessageID, receiveResponse.Messages[0].MessageID)
			assert.Equal(t, "zero visibility", receiveResponse.Messages[0].Body)
			handle := receiveResponse.Messages[0].ReceiptHandle
			require.GreaterOrEqual(t, len(handle), 64)
			if previous, ok := seen[handle[:64]]; ok {
				assert.NotEqual(t, previous, handle, "receipt reused for the same message and deadline")
				sameDeadline = true
				earlierReceiptHandle = previous
				staleReceiptHandle = handle
				break
			}
			seen[handle[:64]] = handle
		}
		require.True(t, sameDeadline, "did not receive the message twice in the same epoch")
	})
	t.Run(`DeleteMessageWithEarlierZeroVisibilityReceipt`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "DeleteMessage", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+earlierReceiptHandle+`"}`)
		require.Equal(t, http.StatusBadRequest, res.StatusCode, string(body))
		assert.Equal(t, `Receipt handle is invalid`, string(body))
	})
	t.Run(`ChangeMessageVisibilityWithEarlierZeroVisibilityReceipt`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "ChangeMessageVisibility", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+earlierReceiptHandle+`","VisibilityTimeout":60}`)
		require.Equal(t, http.StatusBadRequest, res.StatusCode, string(body))
		assert.Equal(t, `Receipt handle is invalid`, string(body))
	})
	t.Run(`ReceiveMessageAgainAfterZeroVisibility`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		var receiveResponse struct {
			Messages []struct {
				MessageID     string `json:"MessageId"`
				ReceiptHandle string `json:"ReceiptHandle"`
			} `json:"Messages"`
		}
		require.NoError(t, json.Unmarshal(body, &receiveResponse))
		require.Len(t, receiveResponse.Messages, 1)
		assert.Equal(t, zeroVisibilityMessageID, receiveResponse.Messages[0].MessageID)
		receiptHandle = receiveResponse.Messages[0].ReceiptHandle
	})
	t.Run(`DeleteMessageWithStaleZeroVisibilityReceipt`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "DeleteMessage", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+staleReceiptHandle+`"}`)
		require.Equal(t, http.StatusBadRequest, res.StatusCode, string(body))
		assert.Equal(t, `Receipt handle is invalid`, string(body))
	})
	t.Run(`DeleteMessageWithTruncatedReceipt`, func(t *testing.T) {
		require.GreaterOrEqual(t, len(receiptHandle), 64)
		res, body := requestSQS(t, c, addr, "DeleteMessage", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+receiptHandle[:64]+`"}`)
		require.Equal(t, http.StatusBadRequest, res.StatusCode, string(body))
		assert.Equal(t, `Receipt handle is invalid`, string(body))
	})
	t.Run(`DeleteMessageWithModifiedReceipt`, func(t *testing.T) {
		require.GreaterOrEqual(t, len(receiptHandle), 80)
		for _, offset := range []int{48, 64} {
			modified := []byte(receiptHandle)
			if modified[offset] == '0' {
				modified[offset] = '1'
			} else {
				modified[offset] = '0'
			}
			res, body := requestSQS(t, c, addr, "DeleteMessage", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+string(modified)+`"}`)
			require.Equal(t, http.StatusBadRequest, res.StatusCode, string(body))
			assert.Equal(t, `Receipt handle is invalid`, string(body))
		}
	})
	t.Run(`ChangeMessageVisibilityBeforeDelete`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "ChangeMessageVisibility", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+receiptHandle+`","VisibilityTimeout":120}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		assert.Equal(t, `{}`, string(body))
	})
	t.Run(`DeleteMessageAfterZeroVisibility`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "DeleteMessage", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+receiptHandle+`"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		assert.Equal(t, `{}`, string(body))
	})
	t.Run(`SetQueueDelayForMessage`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "SetQueueAttributes", `{"QueueUrl":"`+queueURL+`","Attributes":{"DelaySeconds":"2"}}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		assert.Equal(t, `{}`, string(body))
	})
	t.Run(`SendDelayedMessage`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"delayed kitsune"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		var sendResponse struct {
			MessageID string `json:"MessageId"`
		}
		require.NoError(t, json.Unmarshal(body, &sendResponse))
		require.NotEmpty(t, sendResponse.MessageID)
		delayedMessageID = sendResponse.MessageID
	})
	t.Run(`ReceiveDelayedMessageBeforeReady`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		assert.Equal(t, `{"Messages":[]}`, string(body))
	})
	t.Run(`ReceiveDelayedMessageAfterReady`, func(t *testing.T) {
		time.Sleep(3 * time.Second)
		res, body := requestSQS(t, c, addr, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		var receiveResponse struct {
			Messages []struct {
				MessageID     string `json:"MessageId"`
				ReceiptHandle string `json:"ReceiptHandle"`
				Body          string `json:"Body"`
			} `json:"Messages"`
		}
		require.NoError(t, json.Unmarshal(body, &receiveResponse))
		require.Len(t, receiveResponse.Messages, 1)
		assert.Equal(t, delayedMessageID, receiveResponse.Messages[0].MessageID)
		assert.Equal(t, "delayed kitsune", receiveResponse.Messages[0].Body)
		receiptHandle = receiveResponse.Messages[0].ReceiptHandle
	})
	t.Run(`DeleteDelayedMessage`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "DeleteMessage", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+receiptHandle+`"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		assert.Equal(t, `{}`, string(body))
	})
	t.Run(`ResetQueueDelayAfterMessage`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "SetQueueAttributes", `{"QueueUrl":"`+queueURL+`","Attributes":{"DelaySeconds":"0"}}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
	})
	t.Run(`SendMessageBatchInvalidRequests`, func(t *testing.T) {
		entries := make([]map[string]any, 11)
		for i := range entries {
			entries[i] = map[string]any{"Id": strconv.Itoa(i), "MessageBody": "message"}
		}
		cases := []struct {
			name    string
			url     string
			entries any
			code    string
		}{
			{"empty", queueURL, []any{}, "EmptyBatchRequest"},
			{"too many", queueURL, entries, "TooManyEntriesInBatchRequest"},
			{"duplicate", queueURL, []any{map[string]any{"Id": "same", "MessageBody": "must not be sent"}, map[string]any{"Id": "same", "MessageBody": "also invalid"}}, "BatchEntryIdsNotDistinct"},
			{"invalid characters", queueURL, []any{map[string]any{"Id": "bad.id"}}, "InvalidBatchEntryId"},
			{"non-ascii id", queueURL, []any{map[string]any{"Id": "é"}}, "InvalidBatchEntryId"},
			{"empty id", queueURL, []any{map[string]any{"Id": ""}}, "InvalidBatchEntryId"},
			{"long id", queueURL, []any{map[string]any{"Id": strings.Repeat("a", 81)}}, "InvalidBatchEntryId"},
			{"too large", queueURL, []any{map[string]any{"Id": "a", "MessageBody": strings.Repeat("a", (1<<19)+1)}, map[string]any{"Id": "b", "MessageBody": strings.Repeat("b", 1<<19)}}, "BatchRequestTooLong"},
			{"missing queue", queueURL + "-missing", []any{map[string]any{"Id": "a", "MessageBody": "message"}}, "QueueDoesNotExist"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				request, err := json.Marshal(map[string]any{"QueueUrl": tc.url, "Entries": tc.entries})
				require.NoError(t, err)
				res, body := requestSQS(t, c, addr, "SendMessageBatch", string(request))
				require.Equal(t, http.StatusBadRequest, res.StatusCode, string(body))
				require.Equal(t, "application/x-amz-json-1.0", res.Header.Get("Content-Type"))
				var response struct {
					Type string `json:"__type"`
				}
				require.NoError(t, json.Unmarshal(body, &response))
				assert.Equal(t, "com.amazonaws.sqs#"+tc.code, response.Type)
			})
		}
	})
	t.Run(`ReceiveAfterInvalidBatches`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		assert.Equal(t, `{"Messages":[]}`, string(body))
	})
	t.Run(`SendMessageBatchAtEntryLimit`, func(t *testing.T) {
		entries := make([]map[string]any, 10)
		for i := range entries {
			entries[i] = map[string]any{"Id": strconv.Itoa(i), "MessageBody": "message"}
		}
		request, err := json.Marshal(map[string]any{"QueueUrl": queueURL, "Entries": entries})
		require.NoError(t, err)
		res, body := requestSQS(t, c, addr, "SendMessageBatch", string(request))
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		var response struct {
			Successful []json.RawMessage `json:"Successful"`
			Failed     []json.RawMessage `json:"Failed"`
		}
		require.NoError(t, json.Unmarshal(body, &response))
		require.Len(t, response.Successful, 10)
		assert.NotNil(t, response.Failed)
		assert.Empty(t, response.Failed)
	})
	t.Run(`SendMessageBatchAtSizeLimit`, func(t *testing.T) {
		request, err := json.Marshal(map[string]any{
			"QueueUrl": queueURL,
			"Entries": []any{
				map[string]any{"Id": strings.Repeat("a", 80), "MessageBody": strings.Repeat("a", 1<<19)},
				map[string]any{"Id": "second", "MessageBody": strings.Repeat("b", 1<<19)},
			},
		})
		require.NoError(t, err)
		res, body := requestSQS(t, c, addr, "SendMessageBatch", string(request))
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		var response struct {
			Successful []struct {
				ID string `json:"Id"`
			} `json:"Successful"`
			Failed []json.RawMessage `json:"Failed"`
		}
		require.NoError(t, json.Unmarshal(body, &response))
		require.Len(t, response.Successful, 2)
		assert.NotNil(t, response.Failed)
		assert.Empty(t, response.Failed)
	})
	t.Run(`PurgeSizeLimitBatch`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "PurgeQueue", `{"QueueUrl":"`+queueURL+`"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
	})
	t.Run(`SendMessageInvalidContents`, func(t *testing.T) {
		for _, message := range []string{"", "bad\u0001", "bad\uffff"} {
			request, err := json.Marshal(map[string]any{"QueueUrl": queueURL, "MessageBody": message})
			require.NoError(t, err)
			res, body := requestSQS(t, c, addr, "SendMessage", string(request))
			require.Equal(t, http.StatusBadRequest, res.StatusCode, string(body))
		}
	})
	t.Run(`SetQueueDelayForBatch`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "SetQueueAttributes", `{"QueueUrl":"`+queueURL+`","Attributes":{"DelaySeconds":"2"}}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
	})
	batchIDs := make(map[string]string)
	t.Run(`SendMessageBatch`, func(t *testing.T) {
		request := `{"QueueUrl":"` + queueURL + `","Entries":[` +
			`{"Id":"first","MessageBody":"first message","DelaySeconds":0},` +
			`{"Id":"second","MessageBody":"second message","DelaySeconds":0},` +
			`{"Id":"delay","MessageBody":"invalid delay","DelaySeconds":901},` +
			`{"Id":"contents","MessageBody":"invalid\u0001"},` +
			`{"Id":"missing"},` +
			`{"Id":"empty","MessageBody":""},` +
			`{"Id":"attributes","MessageBody":"unsupported","MessageAttributes":{"a":{"DataType":"String","StringValue":"x"}}}]}`
		res, body := requestSQS(t, c, addr, "SendMessageBatch", request)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		var response struct {
			Successful []struct {
				ID        string `json:"Id"`
				MessageID string `json:"MessageId"`
				MD5       string `json:"MD5OfMessageBody"`
			} `json:"Successful"`
			Failed []struct {
				ID          string `json:"Id"`
				Code        string `json:"Code"`
				Message     string `json:"Message"`
				SenderFault bool   `json:"SenderFault"`
			} `json:"Failed"`
		}
		require.NoError(t, json.Unmarshal(body, &response))
		require.Len(t, response.Successful, 2)
		for _, entry := range response.Successful {
			require.Contains(t, map[string]string{"first": "first message", "second": "second message"}, entry.ID)
			require.NotEmpty(t, entry.MessageID)
			digest := md5.Sum([]byte(entry.ID + " message"))
			assert.Equal(t, fmt.Sprintf("%x", digest), entry.MD5)
			batchIDs[entry.ID] = entry.MessageID
		}
		require.Len(t, batchIDs, 2)
		require.NotEqual(t, batchIDs["first"], batchIDs["second"])
		require.Len(t, response.Failed, 5)
		expected := map[string]string{
			"delay": "InvalidParameterValue", "contents": "InvalidMessageContents",
			"missing": "InvalidParameterValue", "empty": "InvalidParameterValue", "attributes": "UnsupportedOperation",
		}
		for _, entry := range response.Failed {
			assert.Equal(t, expected[entry.ID], entry.Code)
			assert.NotEmpty(t, entry.Message)
			assert.True(t, entry.SenderFault)
			delete(expected, entry.ID)
		}
		assert.Empty(t, expected)
	})
	var batchReceipts []string
	t.Run(`ReceiveMessageBatchResults`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`","MaxNumberOfMessages":10}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		var response struct {
			Messages []struct {
				MessageID     string `json:"MessageId"`
				ReceiptHandle string `json:"ReceiptHandle"`
				Body          string `json:"Body"`
			} `json:"Messages"`
		}
		require.NoError(t, json.Unmarshal(body, &response))
		require.Len(t, response.Messages, 2)
		seen := map[string]bool{}
		for _, message := range response.Messages {
			seen[message.Body] = true
			switch message.Body {
			case "first message":
				assert.Equal(t, batchIDs["first"], message.MessageID)
			case "second message":
				assert.Equal(t, batchIDs["second"], message.MessageID)
			default:
				t.Errorf("unexpected message body: %q", message.Body)
			}
			require.NotEmpty(t, message.ReceiptHandle)
			batchReceipts = append(batchReceipts, message.ReceiptHandle)
		}
		require.Len(t, seen, 2)
	})
	t.Run(`DeleteMessageBatchResults`, func(t *testing.T) {
		for _, receipt := range batchReceipts {
			res, body := requestSQS(t, c, addr, "DeleteMessage", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+receipt+`"}`)
			require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		}
	})
	t.Run(`SendMessageWithZeroDelay`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"no delay","DelaySeconds":0}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		var response struct {
			MessageID string `json:"MessageId"`
		}
		require.NoError(t, json.Unmarshal(body, &response))
		require.NotEmpty(t, response.MessageID)
		messageID = response.MessageID
	})
	t.Run(`ReceiveMessageWithZeroDelay`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		var response struct {
			Messages []struct {
				MessageID     string `json:"MessageId"`
				ReceiptHandle string `json:"ReceiptHandle"`
			} `json:"Messages"`
		}
		require.NoError(t, json.Unmarshal(body, &response))
		require.Len(t, response.Messages, 1)
		assert.Equal(t, messageID, response.Messages[0].MessageID)
		require.NotEmpty(t, response.Messages[0].ReceiptHandle)
		receiptHandle = response.Messages[0].ReceiptHandle
	})
	t.Run(`DeleteMessageWithZeroDelay`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "DeleteMessage", `{"QueueUrl":"`+queueURL+`","ReceiptHandle":"`+receiptHandle+`"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
	})
	t.Run(`ResetQueueDelayAfterBatch`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "SetQueueAttributes", `{"QueueUrl":"`+queueURL+`","Attributes":{"DelaySeconds":"0"}}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
	})
	t.Run(`SendMessageForPurge`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "SendMessage", `{"QueueUrl":"`+queueURL+`","MessageBody":"purge me"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
	})
	t.Run(`PurgeQueue`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "PurgeQueue", `{"QueueUrl":"`+queueURL+`"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		assert.Equal(t, `{}`, string(body))
	})
	t.Run(`ReceiveAfterPurge`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "ReceiveMessage", `{"QueueUrl":"`+queueURL+`"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		assert.Equal(t, `{"Messages":[]}`, string(body))
	})
	t.Run(`DeleteQueue`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "DeleteQueue", `{"QueueUrl":"`+queueURL+`"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		assert.Equal(t, `{}`, string(body))
	})
	t.Run(`ListQueuesAfterDelete`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "ListQueues", `{"QueueNamePrefix":"kitsune-test"}`)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		assert.Equal(t, `{"QueueUrls":[]}`, string(body))
	})
	t.Run(`GetQueueUrlAfterDelete`, func(t *testing.T) {
		res, body := requestSQS(t, c, addr, "GetQueueUrl", `{"QueueName":"kitsune-test-queue"}`)
		require.Equal(t, http.StatusBadRequest, res.StatusCode, string(body))
		assert.Equal(t, `Queue does not exist`, string(body))
	})
	// More tests go here
}

func BenchmarkBasic(b *testing.B) {
	c, addr := setup(b)
	res, body := requestSQS(b, c, addr, "CreateQueue", `{"QueueName":"kitsune-benchmark"}`)
	require.Equal(b, http.StatusOK, res.StatusCode, string(body))
	var queue struct {
		QueueURL string `json:"QueueUrl"`
	}
	require.NoError(b, json.Unmarshal(body, &queue))
	require.NotEmpty(b, queue.QueueURL)

	sendBody := `{"QueueUrl":"` + queue.QueueURL + `","MessageBody":"hello kitsune"}`
	receiveBody := `{"QueueUrl":"` + queue.QueueURL + `"}`
	for b.Loop() {
		res, body = requestSQS(b, c, addr, "SendMessage", sendBody)
		require.Equal(b, http.StatusOK, res.StatusCode, string(body))
		var sent struct {
			MessageID string `json:"MessageId"`
		}
		require.NoError(b, json.Unmarshal(body, &sent))
		require.NotEmpty(b, sent.MessageID)

		res, body = requestSQS(b, c, addr, "ReceiveMessage", receiveBody)
		require.Equal(b, http.StatusOK, res.StatusCode, string(body))
		var received struct {
			Messages []struct {
				MessageID     string `json:"MessageId"`
				ReceiptHandle string `json:"ReceiptHandle"`
				Body          string `json:"Body"`
			} `json:"Messages"`
		}
		require.NoError(b, json.Unmarshal(body, &received))
		require.Len(b, received.Messages, 1)
		require.Equal(b, sent.MessageID, received.Messages[0].MessageID)
		require.Equal(b, "hello kitsune", received.Messages[0].Body)
		require.NotEmpty(b, received.Messages[0].ReceiptHandle)

		deleteBody := `{"QueueUrl":"` + queue.QueueURL + `","ReceiptHandle":"` + received.Messages[0].ReceiptHandle + `"}`
		res, body = requestSQS(b, c, addr, "DeleteMessage", deleteBody)
		require.Equal(b, http.StatusOK, res.StatusCode, string(body))
	}
}

func requestSQS(t testing.TB, c *http.Client, addr, action, body string) (*http.Response, []byte) {
	t.Helper()
	var res *http.Response
	var err error
	timeout(t, time.Second, func() {
		res, err = postSQS(c, addr, action, body)
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	responseBody, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.NoError(t, res.Body.Close())
	return res, responseBody
}

func postSQS(c *http.Client, addr, action, body string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/", addr), bytes.NewReader([]byte(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS."+action)
	req.Header.Set("X-Forwarded-Host", addr)
	return c.Do(req)
}

var (
	setupOnce   sync.Once
	setupClient *http.Client
	setupAddr   string
)

func setup(t testing.TB) (*http.Client, string) {
	t.Helper()
	setupOnce.Do(func() {
		setupClient, setupAddr = setupCluster(t)
	})
	require.NotNil(t, setupClient, "cluster setup failed")
	return setupClient, setupAddr
}

func setupCluster(t testing.TB) (*http.Client, string) {
	t.Helper()
	run := func(t testing.TB, name string, fn func(testing.TB)) {
		if test, ok := t.(*testing.T); ok {
			if !test.Run(name, func(t *testing.T) { fn(t) }) {
				test.FailNow()
			}
		} else {
			fn(t)
		}
	}
	var cfg = cluster.EnvConfig()
	cfg.HostName = "127.0.0.1"
	// cfg.LogLevel = "debug"
	var (
		basedir = `./tmp/kitsune`
		hosts   = []*cluster.Host{}
		ctx     = context.Background()
		err     error
		peers   = []string{
			cfg.HostName + `:18011`,
			cfg.HostName + `:18021`,
			cfg.HostName + `:18031`,
		}
		n = 0
	)
	require.NoError(t, os.RemoveAll(basedir))
	start := func(peers []string, class string, portSets ...[]uint16) {
		for i, ports := range portSets {
			cfg := cfg
			cfg.DirBlock = fmt.Sprintf(basedir+"/%d/block", n)
			cfg.DirLocal = fmt.Sprintf(basedir+"/%d/local", n)
			cfg.DirRaft = fmt.Sprintf(basedir+`/%d/raft`, n)
			cfg.DirWAL = fmt.Sprintf(basedir+`/%d/wal`, n)
			cfg.PortZongzi = ports[0]
			cfg.PortRaft = ports[1]
			cfg.PortGossip = ports[2]
			cfg.PortApi = ports[3]
			cfg.HostTags = strings.Join([]string{
				`cluster:member`,
				`cluster:host`,
				`host:class=` + class,
				`geo:zone=test-` + strconv.Itoa(i%3),
				`host:id=` + strconv.Itoa(n),
			}, "|")
			cfg.HostPeers = strings.Join(peers, ",")
			host := cluster.New(ctx, cfg)

			kitsune.Register(host)

			go func() {
				if err := host.Start(); err != nil {
					panic(err)
				}
			}()
			hosts = append(hosts, host)
			n++
		}
		time.Sleep(time.Second)
	}
	run(t, `start`, func(t testing.TB) {
		start(peers, `storage`,
			[]uint16{18011, 18012, 18013, 19010},
			[]uint16{18021, 18022, 18023, 19020},
			[]uint16{18031, 18032, 18033, 19030},
		)
		start(peers, `service`,
			[]uint16{18111, 18112, 18113, 19040},
		)
	})
	host := hosts[0]
	ctrlClient := host.ControlClient()
	require.True(t, await(10, 100, func() bool {
		res, err := ctrlClient.NamespaceList(ctx, &pb.NamespaceListRequest{})
		return err == nil && res.Code == cluster.ResultSuccess && len(res.Item) > 0
	}), `%v`, host)
	port := 19100
	addr := fmt.Sprintf("localhost:%d", port)
	run(t, `provision`, func(t testing.TB) {
		resource := &pb.Resource{
			Namespace: "default",
			Name:      "kitsune-test",
			Type:      "pantopic/kitsune",
		}
		service := &pb.Service{
			Namespace: "default",
			Resource:  resource.Name,
			Cluster:   cfg.ClusterName,
			Name:      "api",
			Type:      "http",
			Port:      uint64(port),
			Status:    cluster.Status_Active,
			Tags: map[string]string{
				"placement:cover": "host:class=service",
			},
		}
		component := &pb.Component{
			Namespace: "default",
			Resource:  resource.Name,
			Cluster:   cfg.ClusterName,
			Name:      "default",
			Type:      "basic",
			Tags: map[string]string{
				"placement:member": "3;host:class=storage",
				"placement:vary":   "geo:zone",
			},
		}
		run(t, `resource`, func(t testing.TB) {
			putResp, err := ctrlClient.ResourcePut(ctx, &pb.ResourcePutRequest{
				Item: resource,
			})
			require.Nil(t, err, err)
			require.Equal(t, uint64(0), putResp.Code, putResp.Error)
			require.Equal(t, resource.Namespace, putResp.Item.Namespace)
			require.Equal(t, resource.Name, putResp.Item.Name)
			require.Equal(t, resource.Type, putResp.Item.Type)
		})
		run(t, `service`, func(t testing.TB) {
			putResp, err := ctrlClient.ServicePut(ctx, &pb.ServicePutRequest{
				Item: service,
			})
			require.Nil(t, err, err)
			require.Equal(t, uint64(0), putResp.Code, putResp.Error)
			require.Equal(t, service.Namespace, putResp.Item.Namespace)
			require.Equal(t, service.Name, putResp.Item.Name)
			require.Equal(t, service.Type, putResp.Item.Type)
		})
		run(t, `component`, func(t testing.TB) {
			putResp, err := ctrlClient.ComponentPut(ctx, &pb.ComponentPutRequest{
				Item: component,
			})
			require.Nil(t, err, err)
			require.Equal(t, uint64(0), putResp.Code, putResp.Error)
			require.Equal(t, component.Namespace, putResp.Item.Namespace)
			require.Equal(t, component.Name, putResp.Item.Name)
			require.Equal(t, component.Resource, putResp.Item.Resource)
			require.Equal(t, component.Type, putResp.Item.Type)
		})
		run(t, `active`, func(t testing.TB) {
			run(t, `service`, func(t testing.TB) {
				svcListReq := &pb.ServiceListRequest{
					Namespace: resource.Namespace,
					Resource:  resource.Name,
				}
				var svcListResp *pb.ServiceListResponse
				// 10s - service is created and active
				require.True(t, await(10, 100, func() bool {
					svcListResp, err = ctrlClient.ServiceList(ctx, svcListReq)
					return err == nil &&
						svcListResp.Code == cluster.ResultSuccess &&
						len(svcListResp.Item) > 0 &&
						svcListResp.Item[0].Status == cluster.Status_Active
				}), "%#v, %#v", svcListResp, err)
				require.Equal(t, 1, len(svcListResp.Item))
				require.Equal(t, service.Type, svcListResp.Item[0].Type)
			})
			run(t, `component`, func(t testing.TB) {
				compListReq := &pb.ComponentListRequest{
					Namespace: resource.Namespace,
					Resource:  resource.Name,
				}
				var compListResp *pb.ComponentListResponse
				// 10s - component is created and active
				require.True(t, await(10, 100, func() bool {
					compListResp, err = ctrlClient.ComponentList(ctx, compListReq)
					return err == nil &&
						compListResp.Code == cluster.ResultSuccess &&
						len(compListResp.Item) > 0 &&
						compListResp.Item[0].Status == cluster.Status_Active
				}), err)
				require.Equal(t, 1, len(compListResp.Item))
				require.Equal(t, component.Type, compListResp.Item[0].Type)
			})
		})
	})
	var c = &http.Client{}
	var res *http.Response
	run(t, `healthcheck`, func(t testing.TB) {
		assert.True(t, await(5, 10, func() bool {
			res, err = c.Get(fmt.Sprintf("http://%s/health", addr))
			return err == nil && res.StatusCode == 200
		}), "timeout")
		require.Nil(t, err, err)
		require.NotNil(t, res)
		body, err := io.ReadAll(res.Body)
		require.Equal(t, http.StatusOK, res.StatusCode, "%s", string(body))
		require.Nil(t, err, err)
		require.Equal(t, "Okay", string(body))
		defer res.Body.Close()
	})
	return c, addr
}

func timeout(t testing.TB, d time.Duration, fn func()) {
	done := make(chan bool)
	go func() {
		fn()
		close(done)
	}()
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		panic(`Timeout`)
	}
}

func await(d, n time.Duration, fn func() bool) bool {
	for range n {
		if fn() {
			return true
		}
		time.Sleep(d * time.Second / n)
	}
	return false
}
