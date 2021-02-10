/**
* @Author:zhoutao
* @Date:2021/2/10 上午9:41
* @Desc:
 */

package tcp

import (
	"context"
	"fmt"
	"github.com/ztaoing/GoRedisDemo/interface/tcp"
	"github.com/ztaoing/GoRedisDemo/lib/logger"
	"github.com/ztaoing/GoRedisDemo/lib/sync/atomic"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
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
		logger.Fatal(fmt.Sprintf("listen err:%v", err))
	}

	// cancel signal
	var closing atomic.AtomicBool
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigChan
		switch sig {
		case syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT:
			logger.Info("signal to shut down...")
			closing.Set(true)
			//listener.Accept() will refuse any connections
			_ = listener.Close()
			//close current connections
			_ = handler.Close()
		}
	}()

	//listen
	logger.Info(fmt.Sprintf("bind:%s, start listening...", cfg.Address))
	defer func() {
		_ = listener.Close()
		_ = handler.Close()
	}()

	//make a root context
	ctx, _ := context.WithCancel(context.Background())
	var waitDone sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			if closing.Get() {
				logger.Info("waiting disconnect...")
				waitDone.Wait()
				//handler will be closed by defer
				return
			}
			logger.Error(fmt.Errorf("accept err:%v", err))
			continue
		}

		logger.Info("accept link")
		waitDone.Add(1)
		//handle the request by goroutine
		go func() {
			defer func() {
				waitDone.Done()
			}()
			handler.Handle(ctx, conn)
		}()
	}

}
