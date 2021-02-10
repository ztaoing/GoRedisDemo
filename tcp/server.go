/**
* @Author:zhoutao
* @Date:2021/2/10 上午9:41
* @Desc:
 */

package tcp

import "time"

type Config struct {
	Address       string        `yaml:"address"`
	MaxConnection uint32        `yaml:"max-connection"`
	Timeout       time.Duration `yaml:"timeout"`
}

//func ListenAndServe(cfg *Config,handler handler)
