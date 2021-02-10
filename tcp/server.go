/**
* @Author:zhoutao
* @Date:2021/2/10 上午9:41
* @Desc:
 */

package tcp

import (
	"github.com/ztaoing/GoRedisDemo/interface/tcp"
	"net"
	"time"
)

type Config struct {
	Address       string        `yaml:"address"`
	MaxConnection uint32        `yaml:"max-connection"`
	Timeout       time.Duration `yaml:"timeout"`
}

func ListenAndServe(cfg *Config, handler tcp.Handler) {
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {

	}
}
