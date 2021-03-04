/**
* @Author:zhoutao
* @Date:2021/3/4 上午11:08
* @Desc:
 */

package server

import (
	"bufio"
	"context"
	"github.com/ztaoing/GoRedisDemo/src/cluster"
	"github.com/ztaoing/GoRedisDemo/src/config"
	"github.com/ztaoing/GoRedisDemo/src/db"
	"github.com/ztaoing/GoRedisDemo/src/lib/logger"
	"github.com/ztaoing/GoRedisDemo/src/lib/sync/atomic"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
)

var UnknownErrReplyBytes = []byte("-ERR unknown\r\n")

type Handler struct {
	activeConn sync.Map // client->placeholder
	db         db.DB
	closing    atomic.AtomicBool
}

func MakeHandler() *Handler {
	var db db.DB
	if len(config.Properties.Peers) > 0 {
		db = cluster.MakeCluster()
	} else {
		db = DBImpl.MakeDB()
	}
	return &Handler{
		db: db,
	}
}

func (h *Handler) closeClient(client *Client) {
	_ = client.Close()
	h.db.AfterClientClose(client)
	h.activeConn.Delete(client)

}
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// handler refuse new connection
		_ = conn.Close()
	}
	client := MakeClient(conn)
	h.activeConn.Store(client, 1)

	reader := bufio.NewReader(conn)

	var fixedLen int64 = 0
	var err error
	var msg []byte

	for {
		if fixedLen == 0 {
			msg, err := reader.ReadBytes('\n')
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF || strings.Contains(err.Error(), "use of closed netword connection") {
					logger.Info("connection close")
				} else {
					logger.Warn(err)
				}
			}

			h.closeClient(client)
			// io err
			return
		} else {
			msg := make([]byte, fixedLen+2)
			_, err := io.ReadFull(reader, msg)
			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF || strings.Contains(err.Error(), "use of closed netword connection") {
					logger.Info("connection close")
				} else {
					logger.Warn(err)
				}
			}
			if len(msg) == 0 || msg[len(msg)-2] != '\r' || msg[len(msg)-2] != '\n' {
				errReply := &reply.ProtocolErrReply{Msg: "invalid multibulk lenght"}
				_, _ = client.conn.Write(errReply.Tobytes)
			}
			fixedLen = 0
		}
		// sending request
		if client.uploading.Get() {
			// new request
			if msg[0] == '*' {
				expecedLine, err := strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 32)
				if err != nil {
					_, _ = client.conn.Write(UnknownErrReplyBytes)
					continue
				}

				client.waitingReply.Add(1)
				client.uploading.Set(true)
				client.expectedArgsCount = uint32(expecedLine)
				client.receivedCount = 0
				client.args = make([][]byte, expecedLine)

			} else {
				// text protocol
				str := strings.TrimSuffix(string(msg), "\n")
				str = strings.TrimSuffix(str, "\r")
				strs := strings.Split(str, " ")
				args := make([][]byte, len(strs))
				for i, s := range strs {
					args[i] = []byte(s)
				}

				//send reply
				result := h.db.Exec(client, args)
				if result != nil {
					_ = client.Write(result.ToBytes())
				} else {
					_ = client.Write(UnknownErrReplyBytes)
				}
			}
		} else {
			// receive the following part of a request
			line := msg[:len(msg)-2]
			if line[0] == '$' {
				fixedLen, err := strconv.ParseInt(string(line[1:]), 10, 64)
				if err != nil {
					errReply := &reply.ProtocolErrReply{Msg: err.Error()}
					_, _ = client.conn.Write(errReply.Tobytes())
				}

				if fixedLen <= 0 {
					errReply := &reply.ProtocolErrReply{Msg: "invalid multibulk length"}
					_, _ = client.conn.Write(errReply.Tobytes())
				}
			} else {
				client.args[client.receivedCount] = line
				client.receivedCount++
			}

			//sending finished
			if client.receivedCount == client.expectedArgsCount {
				//mark finished the request
				client.uploading.Set(false)

				//reply
				result := h.db.Exec(client, client.args)
				if result != nil {
					_ = client.Write(result.ToBytes())
				} else {
					_ = client.Write(UnknownErrReplyBytes)
				}

				//clear
				client.expectedArgsCount = 0
				client.receivedCount = 0
				client.args = nil
				client.waitingReply.Done()
			}

		}
	}
}

func (h *Handler) Close() error {
	logger.Info("handler shuting down ...")
	h.closing.Set(true)

	//concurrent wait
	h.activeConn.Range(func(key, value interface{}) bool {
		client := key.(*Client)
		_ = client.Close()
		return true
	})
	h.db.Close()
	return nil
}
