/**
* @Author:zhoutao
* @Date:2021/3/2 下午12:35
* @Desc:DEL,MSET等命令所操作的key可能分布在不同的节点上。全部成功或者全部失败。
 */

package cluster

import (
	"fmt"
	"github.com/ztaoing/GoRedisDemo/src/interface/redis"
	"github.com/ztaoing/GoRedisDemo/src/redis/reply"
)

func MGet(c *Cluster, conn redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return reply.MakeErrReply("error : wrong number of arguments for 'mget' ")
	}
	//从参数列表中取出要操作的key
	keys := make([]string, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		keys[i] = string(args[i+1])
	}
	resultMap := make(map[string][]byte)
	//计算每个key所在的节点，并按照节点分组
	groupMap := c.groupBy(keys)

	for peer, group := range groupMap {
		//向每个节点上发送mget命令
		resp := c.Relay(peer, conn, makeArgs("MGet", group...))
		if reply.IsErrorReply(resp) {
			errReply := resp.(reply.ErrorReply)
			return reply.MakeErrReply(fmt.Sprintf("error : error to get %s ,%v", group[0], errReply.Error()))
		}
		arrReply, _ := resp.(*reply.MultiBulkReply)
		for i, v := range arrReply.Args {
			key := group[i]
			resultMap[key] = v
		}
	}

	result := make([][]byte, len(keys))
	//将每个节点上的resp放到result中
	for i, k := range keys {
		result[i] = resultMap[k]
	}
	return reply.MakeMultiBulkReply(result)
}
