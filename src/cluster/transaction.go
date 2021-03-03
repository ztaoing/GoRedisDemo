/**
* @Author:zhoutao
* @Date:2021/3/2 下午1:15
* @Desc:
 */

package cluster

import (
	"fmt"
	"github.com/ztaoing/GoRedisDemo/src/db"
	"github.com/ztaoing/GoRedisDemo/src/interface/redis"
	"github.com/ztaoing/GoRedisDemo/src/lib/marshal/gob"
	"github.com/ztaoing/GoRedisDemo/src/redis/reply"
	"strconv"
	"strings"
	"time"
)

const (
	maxLockTime   = 3 * time.Second
	CreatedStatus = iota
	PreparedStatus
	CommitedStatus
	RollbackStatus
)

type Transaction struct {
	id      string            // 事务ID，snowflake
	args    [][]byte          //命令参数
	keys    []string          //事务中涉及的key
	undolog map[string][]byte //保存每个key及事务执行之前的值，用于回滚事务
	cluster *Cluster
	conn    redis.Connection
	status  int8
}

func NewTransaction(c *Cluster, conn redis.Connection, TxID string, keys []string, args [][]byte) *Transaction {
	return &Transaction{
		id:      TxID,
		args:    args,
		keys:    keys,
		cluster: c,
		conn:    conn,
		status:  CreatedStatus,
	}
}

//prepare阶段
//PrepareMSet TxID key1,key2 ...
func PrepareMSet(c *Cluster, conn redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 3 {
		return reply.MakeErrReply("error : lack of arguments for 'Prepareset'")
	}
	TxID := string(args[1])
	size := (len(args) - 2) / 2
	keys := make([]string, size)
	//key
	for i := 0; i < size; i++ {
		//args[2*i+2]
		keys[i] = string(args[2*i+2])
	}
	TxArgs := [][]byte{[]byte("MSet")}
	TxArgs = append(TxArgs, args[2:]...)

	//创建新事务
	tx := NewTransaction(c, conn, TxID, keys, args)
	//存储到节点的事务列表中
	c.transaction.Put(TxID, tx)
	//prepare阶段
	err := tx.prepare()
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	return &reply.OKReply{}
}

//准备操作: 将key存入undo log
func (t *Transaction) prepare() error {
	//锁定key
	t.cluster.db.Locks(t.keys...)

	//undo log
	t.undolog = make(map[string][]byte)
	for _, key := range t.keys {
		entity, ok := t.cluster.db.Get(key)
		if ok {
			//将事务修改之前的状态，序列化知道存储到undo log 中
			blob, err := gob.Marshal(entity)
			if err != nil {
				return err
			}
			t.undolog[key] = blob
		} else {
			//如果事务执行前key是不存在的，在回滚时删除它
			t.undolog[key] = []byte{}
		}
	}
	t.status = PreparedStatus
	return nil
}

//协调者
func MSet(c *Cluster, conn redis.Connection, args [][]byte) redis.Reply {
	//parse paremter
	argCount := len(args) - 1
	if argCount%2 != 0 || argCount < 1 {
		return reply.MakeErrReply("error: lack of arguments for 'mset'")
	}
	size := argCount / 2
	keys := make([]string, size)
	valueMap := make(map[string]string)
	for i := 0; i < size; i++ {
		keys[i] = string(args[2*i+1])
		valueMap[keys[i]] = string(args[2*i+2])
	}
	//节点->keys
	groupMap := c.groupBy(keys)
	if len(groupMap) == 1 {
		//所有的keys都在同一个节点上，不用两阶段提交
		for peer := range groupMap {
			return c.Relay(peer, conn, args)
		}
	}

	//开始准备阶段
	var errReply redis.Reply
	//snowflake
	TxID := c.idGenerator.NextID()
	TxIDStr := strconv.FormatInt(TxID, 10)
	rollback := false

	//向所有参与者发送prepare请求
	// goroutine处理,waitgroup等待
	for peer, group := range groupMap {
		peerArgs := []string{TxIDStr}

		for _, k := range group {
			peerArgs = append(peerArgs, k, valueMap[k])
		}
		var resp redis.Reply
		if peer == c.self {
			//本地
			resp = PrepareMSet(c, conn, makeArgs("PrepareMSet", peerArgs...))
		} else {
			//中转到其他节点
			c.Relay(peer, conn, makeArgs("PrepareMSet", peerArgs...))
		}
		if reply.IsErrorReply(resp) {
			errReply = resp
			rollback = true
			break
		}
	}
	//goroutine 超时或者执行错误时，回滚
	if rollback {
		//回滚
		RequestRollback(c, conn, TxID, groupMap)
	} else {
		//提交
		_, errReply = RequestCommit(c, conn, TxID, groupMap)
		rollback = errReply != nil
	}
	if !rollback {
		return &reply.OKReply{}
	}
	return errReply
}

//协调者提交事务
func RequestCommit(c *Cluster, conn redis.Connection, TxID int64, peers map[string][]string) ([]redis.Reply, reply.ErrorReply) {
	var errReply reply.ErrorReply

	TxIDStr := strconv.FormatInt(TxID, 10)

	respList := make([]redis.Reply, 0, len(peers))
	//通知所有相关节点提交
	for peer := range peers {
		var resp redis.Reply
		if peer == c.self {
			resp = Commit(c, conn, makeArgs("commit", TxIDStr))
		} else {
			resp = c.Relay(peer, conn, makeArgs("commit", TxIDStr))
		}
		if reply.IsErrorReply(resp) {
			errReply = resp.(reply.ErrorReply)
			break
		}
		respList = append(respList, resp)
	}
	if errReply != nil {
		RequestRollback(c, conn, TxID, peers)
		return nil, errReply
	}
	return respList, nil
}

//提交
func Commit(c *Cluster, conn redis.Connection, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return reply.MakeErrReply("error : lack of argument for 'commit'")
	}
	//读取事务信息
	TxID := string(args[1])
	raw, ok := c.transaction.Get(TxID)
	if !ok {
		return reply.MakeIntReply(0)
	}
	tx, _ := raw.(*Transaction)

	//提交成功后解锁key
	defer func() {
		c.db.UnLocks(tx.keys...)
		tx.status = CommitedStatus
	}()

	cmd := strings.ToLower(string(tx.args[0]))
	var result redis.Reply

	if cmd == "del" {
		result = commitDel(c, conn, tx)
	} else if cmd == "mset" {
		result = commitMSet(c, conn, tx)
	}

	//提交失败
	if reply.IsErrorReply(result) {
		err2 := tx.rollback()
		return reply.MakeErrReply(fmt.Sprintf("rollback error : %v,origin error: %s", err2, result))
	}
	return result

}

func RequestRollback(c *Cluster, conn redis.Connection, TxID int64, peers map[string][]string) {
	TxIDStr := strconv.FormatInt(TxID, 10)
	for peer := range peers {
		if peer == c.self {
			Rollback(c, makeArgs("rollback", TxIDStr))
		} else {
			//中转
			c.Relay(peer, conn, makeArgs("rollback", TxIDStr))
		}
	}
}

func (t *Transaction) rollback() error {
	for key, blob := range t.undolog {
		if len(blob) > 0 {
			entity := &db.DataEntry{}
			err := gob.UnMarshal(blob, entity)
			if err != nil {
				return err
			}
			//更新为执行事务前的状态
			t.cluster.db.Put(key, entity)
		} else {
			//如果key之前不存在，则删除
			t.cluster.db.Remove(key)
		}
	}
	if t.status != CommitedStatus {
		t.cluster.db.UnLocks(t.keys...)
	}
	t.status = RollbackStatus
	return nil
}

func Rollback(c *Cluster, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return reply.MakeErrReply("Rollback error : lack of arguments for 'rollback'")
	}

	TxID := string(args[1])
	raw, ok := c.transaction.Get(TxID)
	if !ok {
		return reply.MakeIntReply(0)
	}
	tx, _ := raw.(*Transaction)
	err := tx.rollback()
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}

	return reply.MakeIntReply(1)
}
