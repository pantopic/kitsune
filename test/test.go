package kitsune_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func All(t *testing.T, c *http.Client, addr string) {
	var err error
	var res *http.Response
	t.Run(`content-type`, func(t *testing.T) {
		require.True(t, await(10, 100, func() bool {
			res, err = c.Post(fmt.Sprintf("http://%s/", addr), "application/json", nil)
			return err == nil
		}), "%s", err)
		require.Nil(t, err, err)
		require.NotNil(t, res)
		require.Equal(t, res.StatusCode, 400)
		body, err := io.ReadAll(res.Body)
		require.Nil(t, err, err)
		assert.Equal(t, `invalid content-type`, string(body))
	})
	t.Run(`ListQueues`, func(t *testing.T) {
		require.True(t, await(10, 100, func() bool {
			res, err = c.Post(fmt.Sprintf("http://%s/", addr), "application/x-amz-json-1.0", bytes.NewReader([]byte(
				`{"Action":"ListQueues"}`,
			)))
			return err == nil
		}), "%s", err)
		require.Nil(t, err, err)
		require.NotNil(t, res)
		require.Equal(t, 200, res.StatusCode)
		body, err := io.ReadAll(res.Body)
		require.Nil(t, err, err)
		assert.Equal(t, `results`, string(body))
	})
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
