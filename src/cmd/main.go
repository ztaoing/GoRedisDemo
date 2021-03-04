/**
* @Author:zhoutao
* @Date:2021/3/4 上午10:55
* @Desc:
 */

package cmd

import (
	"fmt"
	"github.com/ztaoing/GoRedisDemo/src/config"
	"github.com/ztaoing/GoRedisDemo/src/lib/logger"
	"github.com/ztaoing/GoRedisDemo/src/tcp"
	"os"
)

func main() {
	configFileName := os.Getenv("CONFIG")
	if configFileName == "" {
		configFileName = "redis.conf"
	}
	config.SetupConfig(configFileName)
	logger.Setup(&logger.LogSetting{
		Path:       "",
		Name:       "goredis",
		Ext:        ".log",
		TimeFormat: "2006-01-02",
	})
	tcp.ListenAndServe(&tcp.Config{
		Address: fmt.Sprintf("%s:%d", config.Properties.Bind, config.Properties.Port),
	}, RedisServer.MakeHandler())
}
