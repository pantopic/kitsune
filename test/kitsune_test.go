package kitsune_test

import (
	"bytes"
	"context"
	_ "embed"
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
	var err error
	var res *http.Response
	t.Run(`content-type`, func(t *testing.T) {
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
	t.Run(`ListQueues`, func(t *testing.T) {
		timeout(t, time.Second, func() {
			req, reqErr := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/", addr), bytes.NewReader([]byte(
				`{"QueueName":"kitsune-test-queue"}`,
			)))
			if reqErr != nil {
				err = reqErr
				return
			}
			req.Header.Set("Content-Type", "application/x-amz-json-1.0")
			req.Header.Set("X-Amz-Target", "AmazonSQS.CreateQueue")
			req.Header.Set("X-Forwarded-Host", addr)
			res, err = c.Do(req)
		})

		require.NoError(t, err)
		body, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode, string(body))
		assert.Equal(t, "application/x-amz-json-1.0", res.Header.Get("Content-Type"))
		assert.Contains(t, string(body), `"QueueUrl":"http://`+addr)
		assert.Contains(t, string(body), `/kitsune-test-queue"`)
		require.NoError(t, res.Body.Close())

		timeout(t, time.Second, func() {
			req, reqErr := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/", addr), bytes.NewReader([]byte(
				`{"QueueNamePrefix":"kitsune-test"}`,
			)))
			if reqErr != nil {
				err = reqErr
				return
			}
			req.Header.Set("Content-Type", "application/x-amz-json-1.0")
			req.Header.Set("X-Amz-Target", "AmazonSQS.ListQueues")
			req.Header.Set("X-Forwarded-Host", addr)
			res, err = c.Do(req)
		})
		require.NoError(t, err)
		body, err = io.ReadAll(res.Body)
		require.Equal(t, 200, res.StatusCode, "%s", string(body))
		require.Nil(t, err, err)
		assert.Equal(t, "application/x-amz-json-1.0", res.Header.Get("Content-Type"))
		assert.Contains(t, string(body), `"QueueUrls":["http://`+addr)
		assert.Contains(t, string(body), `/kitsune-test-queue"]`)
		require.NoError(t, res.Body.Close())
	})
	// More tests go here
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
