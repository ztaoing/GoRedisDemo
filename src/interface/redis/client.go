/**
* @Author:zhoutao
* @Date:2021/3/2 上午9:18
* @Desc:
 */

package redis

type Connection interface {
	Write([]byte) error

	//client keeps subscribing channel
	SubChannel(channel string)
	UnSubChannel(channel string)
	SubCount() int
	GetChannel() []string
}
