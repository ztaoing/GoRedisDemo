/**
* @Author:zhoutao
* @Date:2021/2/10 上午9:44
* @Desc:
 */

package tcp

import (
	"context"
	"net"
)

type HandlerFunc func(ctx context.Context, conn net.Conn)

type Handler interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}
