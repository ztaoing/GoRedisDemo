/**
* @Author:zhoutao
* @Date:2021/3/1 下午1:30
* @Desc:
 */

package cluster

import (
	"context"
	"errors"
	"fmt"
	"github.com/jolestar/go-commons-pool/v2"
	"github.com/ztaoing/GoRedisDemo/src/cluster/idgenerator"
	"github.com/ztaoing/GoRedisDemo/src/config"
	"github.com/ztaoing/GoRedisDemo/src/datastruct/dict"
	"github.com/ztaoing/GoRedisDemo/src/db"
	"github.com/ztaoing/GoRedisDemo/src/interface/redis"
	"github.com/ztaoing/GoRedisDemo/src/lib/consisttenthash"
	"github.com/ztaoing/GoRedisDemo/src/lib/logger"
	"github.com/ztaoing/GoRedisDemo/src/redis/client"
	"github.com/ztaoing/GoRedisDemo/src/redis/reply"
	"runtime/debug"
	"strings"
)

type Cluster struct {
	self           string
	peerPicker     *consisttenthash.Map
	peerConnection map[string]*pool.ObjectPool
	db             *db.DB
	transaction    *dict.SimpleDict         //id ->Transaction
	idGenerator    *idgenerator.IDGenerator //snowFlake
}

func MakeCluster() *Cluster {
	cluster := &Cluster{
		self:           config.Properties.Self,
		db:             db.MakeDB(),
		transaction:    dict.MakeSimple(),
		peerPicker:     consistenthash.New(replicas, nil),
		peerConnection: make(map[string]*pool.ObjectPool),
		idGenerator:    idgenerator.MakeGenerator("goRedisDemo", config.Properties.Self),
	}

}

func (c *Cluster) Relay(peer string, conn redis.Connection, args [][]byte) redis.Reply {
	//若数据在本地则直接调用本地数据库
	if peer == c.self {
		return c.db.Exec(conn, args)
	} else {
		//从连接池中获取一个与目标节点的连接
		//连接池使用go-commons-pool/v2
		peerClient, err := c.getPeerClient(peer)
		if err != nil {
			return reply.MakeErrReply(err.Error())
		}
		defer func() {
			//处理完，归还连接到连接池
			c.returnPeerClient(peer, peerClient)
		}()
		// send cmd to remote
		return peerClient.Send(args)
	}

}

//支持的指令
var router = MakeRouter()

type CmdFunc func(c *Cluster, conn redis.Connection, args [][]byte) redis.Reply

//调
func (c *Cluster) Exec(conn redis.Connection, args [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs:%v \n %s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()

	cmd := strings.ToLower(string(args[0]))
	//查询指令
	cmdFunc, ok := router[cmd]
	if !ok {
		return reply.MakeErrReply("Error: unknown command '" + cmd + "',or supported in cluster mode")
	}
	result = cmdFunc(c, conn, args)
	return

}

//冲连接池中获取连接
func (c *Cluster) getPeerClient(peer string) (*client.Client, error) {
	connectionFactory, ok := c.peerConnection[peer]
	if !ok {
		return nil, errors.New("connection not find")
	}

	raw, err := connectionFactory.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}
	conn, ok := raw.(*client.Client)
	if !ok {
		return nil, errors.New("connection factory makes wrong type")
	}
	return conn, nil
}

//归还连接到连接池
func (c *Cluster) returnPeerClient(peer string, peerClient *client.Client) error {
	connectionFactory, ok := c.peerConnection[peer]
	if !ok {
		return errors.New("the connection Factory can not be find")
	}

	return connectionFactory.ReturnObject(context.Background(), peerClient)
}

func Ping(c *Cluster, conn redis.Connection, args [][]byte) redis.Reply {
	if len(args) == 1 {
		return &reply.PongReply{}
	} else if len(args) == 2 {
		return reply.MakeStatusReply("\"" + string(args[1]) + "\"")
	} else {
		return reply.MakeErrReply("ping error : wrong number of arguments")
	}
}

// 节点->keys
func (c *Cluster) groupBy(keys []string) map[string][]string {
	result := make(map[string][]string)
	for _, key := range keys {
		peer := c.peerPicker.Get(key)
		group, ok := result[peer]
		if !ok {
			group = make([]string, 0)
		}
		group = append(group, key)
		result[peer] = group
	}
	return result
}

func makeArgs(cmd string, args ...string) [][]byte {
	result := make([][]byte, len(args)+1)
	result[0] = []byte(cmd)

	for i, arg := range args {
		result[i+1] = []byte(arg)
	}

	return result
}
