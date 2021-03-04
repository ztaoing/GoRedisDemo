/**
* @Author:zhoutao
* @Date:2021/3/4 上午10:53
* @Desc:
 */

package db

import "github.com/ztaoing/GoRedisDemo/src/interface/redis"

type DB interface {
	Exec(c redis.Connection, args [][]byte) redis.Reply
	AfterClientClose(conn redis.Connection)
	Close()
}
