/**
* @Author:zhoutao
* @Date:2021/2/28 下午12:42
* @Desc:
 */

package client

import (
	"bufio"
	"context"
	"errors"
	"github.com/ztaoing/GoRedisDemo/src/interface/redis"
	"github.com/ztaoing/GoRedisDemo/src/lib/logger"
	"github.com/ztaoing/GoRedisDemo/src/lib/sync/wait"
	"github.com/ztaoing/GoRedisDemo/src/redis/reply"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxWait = 3 * time.Second
)

type Client struct {
	conn        net.Conn      //与服务端建立的tcp连接
	sendingReqs chan *Request //等待发送的请求
	waitingReqs chan *Request //等待服务器响应的请求
	ticker      *time.Ticker  //用于触发心跳包的计时器
	addr        string
	ctx         context.Context
	cancelFunc  context.CancelFunc
	writing     *sync.WaitGroup
}

type Request struct {
	id        uint64      //请求id
	args      [][]byte    //参数
	reply     redis.Reply //收到的返回值
	heartbeat bool        //是否是心跳请求
	waiting   *wait.Wait
	err       error
}

//将请求发送给服务端，并等待异步处理完成
func (c *Client) Send(args [][]byte) redis.Reply {
	request := &Request{
		args:      args,
		heartbeat: false,
		waiting:   &wait.Wait{},
	}
	request.waiting.Add(1)
	//将请求发送到处理队列
	c.sendingReqs <- request
	//等待请求处完成或超时
	timeout := request.waiting.WaitWithTimeout(maxWait)
	if timeout {
		return reply.MakeErrReply("server timeout")
	}
	if request.err != nil {
		return reply.MakeErrReply("request failed: " + request.err.Error())
	}
	return request.reply
}

//写goroutine
func (c *Client) handleWrite() {
loop:
	for {
		select {
		//从channel中读取请求
		case req := <-c.sendingReqs:
			//增加请求数量
			c.writing.Add(1)
			//发送请求
			c.doRequest(req)
		case <-c.ctx.Done():
			break loop
		}
	}
}

//发送请求
func (c *Client) doRequest(req *Request) {
	//序列化
	bytes := reply.MakeMultiBulkReply(req.args).ToBytes()
	//使用tcp发送
	_, err := c.conn.Write(bytes)
	i := 0
	//失败重试
	for err != nil && i < 3 {
		err = c.handleConnectionError(err)
		if err == nil {
			_, err = c.conn.Write(bytes)
		}
		//处理失败
		i++
	}
	if err == nil {
		//将已经发送成功的请求放入等地啊响应队列
		c.waitingReqs <- req
	} else {
		//发送失败
		req.err = err
		//结束等待
		req.waiting.Done()
		//未完成请求数减1
		c.writing.Done()
	}
}

func (c *Client) handleConnectionError(err error) error {
	e := c.conn.Close()
	if e != nil {
		if opErr, ok := e.(*net.OpError); ok {
			if opErr.Err.Error() != "use of closed network connection" {
				return e
			}
		} else {
			return e
		}
	}
	conn, e := net.Dial("tcp", c.addr)
	if e != nil {
		logger.Error(e)
		return e
	}
	c.conn = conn
	go func() {
		_ = c.handleRead()
	}()
	return nil
}

func (c *Client) finishRequest(reply redis.Reply) {
	//取出等待响应的request
	request := <-c.waitingReqs
	request.reply = reply
	if request.waiting != nil {
		//结束调用者的等待
		request.waiting.Done()
	}
	//未完成请求数减1
	c.writing.Done()
}

//读goroutine，是RESP协议解析器
func (c *Client) handleRead() error {
	reader := bufio.NewReader(c.conn)
	downloading := false
	expectedArgsCount := 0
	receivedCount := 0
	// fisrt char of msg
	msgType := byte(0)
	var args [][]byte
	var fixedLen int64 = 0
	var err error
	var msg []byte

	for {
		//read
		if fixedLen == 0 {
			//read normal line
			msg, err = reader.ReadBytes('\n')
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					logger.Info("connection closed")
				} else {
					logger.Warn(err)
				}
				return errors.New("connection closed")
			}

			if len(msg) == 0 || msg[len(msg)-2] != '\r' {
				return errors.New("protocol error")
			}
		} else {
			//read bulk line ,binary safe
			msg = make([]byte, fixedLen+2)
			_, err := io.ReadFull(reader, msg)
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					return errors.New("connection closed")
				} else {
					return err
				}
			}
			// read nothing
			if len(msg) == 0 || msg[len(msg)-2] != '\r' || msg[len(msg)-1] != '\n' {
				return errors.New("protocol error")
			}
			fixedLen = 0
		}

		//parse line
		if !downloading {
			//receive new response
			if msg[0] == '*' {
				//multi bulk response
				expectedLine, err := strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
				if err != nil {
					return errors.New("protocol error: " + err.Error())
				}
				if expectedLine == 0 {
					c.finishRequest(&reply.EmptyMultiBulkReply{})
				} else if expectedLine > 0 {
					//消息的类型
					msgType = msg[0]
					downloading = true
					expectedArgsCount = int(expectedLine)
					receivedCount = 0
					args = make([][]byte, expectedLine)
				} else {
					return errors.New("protocol error")
				}
			} else if msg[0] == '$' {
				//bulk response
				fixedLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
				if err != nil {
					return err
				}
				//null
				if fixedLen == -1 {
					c.finishRequest(&reply.NullBulkReply{})
					fixedLen = 0
				} else if fixedLen > 0 {
					msgType = msg[0]
					downloading = true
					expectedArgsCount = 1
					receivedCount = 0
					args = make([][]byte, 1)
				}
			} else {
				//single line response
				str := strings.TrimSuffix(string(msg), "\n")
				str = strings.TrimSuffix(str, "\r")
				var result redis.Reply
				switch msg[0] {
				case '+':
					result = reply.MakeStatusReply(str[1:])
				case '-':
					result = reply.MakeErrReply(str[1:])
				case ':':
					val, err := strconv.ParseInt(str[1:], 10, 64)
					if err != nil {
						return errors.New("protocol error")
					}
					result = reply.MakeIntReply(val)
				}
				c.finishRequest(result)
			}
		} else {
			// receive following part of the request
			line := msg[0 : len(msg)-2]
			if line[0] == '$' {
				fixedLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
				if err != nil {
					return err
				}
				// null bulk in multi bulks
				if fixedLen <= 0 {
					args[receivedCount] = []byte{}
					receivedCount++
					fixedLen = 0
				}
			} else {
				args[receivedCount] = line
				receivedCount++
			}
			// if sending finished
			if receivedCount == expectedArgsCount {
				// 结束下载
				downloading = false
				if msgType == '*' {
					reply := reply.MakeMultiBulkReply(args)
					c.finishRequest(reply)
				} else if msgType == '$' {
					reply := reply.MakeBulkReply(args[0])
					c.finishRequest(reply)
				}
				//finish reply
				expectedArgsCount = 0
				receivedCount = 0
				args = nil
				msgType = byte(0)
			}
		}
	}
}

func MakeClient(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{
		addr:        addr,
		conn:        conn,
		sendingReqs: make(chan *Request, chanSize),
		waitingReqs: make(chan *Request, chanSize),
		ctx:         ctx,
		cancelFunc:  cancel,
		writing:     &sync.WaitGroup{},
	}, nil
}

func (c *Client) Start() {
	c.ticker = time.NewTicker(10 * time.Second)
	go c.handleWrite()
	go func() {
		err := c.handleRead()
		logger.Warn(err)
	}()
	go c.heartbeat()
}

func (c *Client) Close() {
	//阻止新请求进入队列
	close(c.sendingReqs)
	//等待处理中的请求
	c.writing.Wait()
	//释放资源
	_ = c.conn.Close()
	//关闭goroutine
	c.cancelFunc()
	//关闭
	close(c.waitingReqs)
}
