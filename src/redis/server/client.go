/**
* @Author:zhoutao
* @Date:2021/3/4 下午12:03
* @Desc:
 */

package server

import (
	"github.com/ztaoing/GoRedisDemo/src/lib/sync/atomic"
	"github.com/ztaoing/GoRedisDemo/src/lib/sync/wait"
	"net"
	"sync"
	"time"
)

type Client struct {
	conn         net.Conn
	waitingReply wait.Wait
	// sending request
	uploading         atomic.AtomicBool
	expectedArgsCount uint32
	receivedCount     uint32
	args              [][]byte
	mu                sync.Mutex
	subs              map[string]bool
}

func MakeClient(conn net.Conn) *Client {
	return &Client{
		conn: conn,
	}
}

func (c *Client) Close() error {
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	return nil
}

func (c *Client) Write(b []byte) error {
	if b == nil || len(b) == 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := c.conn.Write(b)
	return err
}

func (c *Client) SubChannel(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subs == nil {
		c.subs = make(map[string]bool)
	}
	c.subs[channel] = true
}

func (c *Client) UnSubChannel(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.subs == nil {
		return
	}
	delete(c.subs, channel)
}

func (c *Client) SubsCount() int {
	if c.subs == nil {
		return 0
	}
	return len(c.subs)
}

func (c *Client) GetChannels() []string {
	if c.subs == nil {
		return make([]string, 0)
	}
	channels := make([]string, len(c.subs))
	i := 0
	for channel := range c.subs {
		channels[i] = channel
		i++
	}
	return channels
}
