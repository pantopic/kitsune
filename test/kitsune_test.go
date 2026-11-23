package kitsune_test

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
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
	var receiptHandle string

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
		receiptHandle = receiveResponse.Messages[0].ReceiptHandle
	})
	t.Run(`ChangeMessageVisibilityTimeout`, func(t *testing.T) {
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

func requestSQS(t *testing.T, c *http.Client, addr, action, body string) (*http.Response, []byte) {
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

func setup(t *testing.T) (*http.Client, string) {
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
	os.RemoveAll(basedir)
	start := func(t *testing.T, peers []string, class string, portSets ...[]uint16) {
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
	t.Run(`start`, func(t *testing.T) {
		start(t, peers, `storage`,
			[]uint16{18011, 18012, 18013, 19010},
			[]uint16{18021, 18022, 18023, 19020},
			[]uint16{18031, 18032, 18033, 19030},
		)
		start(t, peers, `service`,
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
	t.Run(`provision`, func(t *testing.T) {
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
		t.Run(`resource`, func(t *testing.T) {
			putResp, err := ctrlClient.ResourcePut(ctx, &pb.ResourcePutRequest{
				Item: resource,
			})
			require.Nil(t, err, err)
			require.Equal(t, uint64(0), putResp.Code, putResp.Error)
			require.Equal(t, resource.Namespace, putResp.Item.Namespace)
			require.Equal(t, resource.Name, putResp.Item.Name)
			require.Equal(t, resource.Type, putResp.Item.Type)
		})
		t.Run(`service`, func(t *testing.T) {
			putResp, err := ctrlClient.ServicePut(ctx, &pb.ServicePutRequest{
				Item: service,
			})
			require.Nil(t, err, err)
			require.Equal(t, uint64(0), putResp.Code, putResp.Error)
			require.Equal(t, service.Namespace, putResp.Item.Namespace)
			require.Equal(t, service.Name, putResp.Item.Name)
			require.Equal(t, service.Type, putResp.Item.Type)
		})
		t.Run(`component`, func(t *testing.T) {
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
		t.Run(`active`, func(t *testing.T) {
			t.Run(`service`, func(t *testing.T) {
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
			t.Run(`component`, func(t *testing.T) {
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
	t.Run(`healthcheck`, func(t *testing.T) {
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

func timeout(t *testing.T, d time.Duration, fn func()) {
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
