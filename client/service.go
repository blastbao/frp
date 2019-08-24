// Copyright 2017 fatedier, fatedier@gmail.com
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fatedier/frp/assets"
	"github.com/fatedier/frp/g"
	"github.com/fatedier/frp/models/config"
	"github.com/fatedier/frp/models/msg"
	"github.com/fatedier/frp/utils/log"
	frpNet "github.com/fatedier/frp/utils/net"
	"github.com/fatedier/frp/utils/util"
	"github.com/fatedier/frp/utils/version"

	fmux "github.com/hashicorp/yamux"
)




type Service struct {

	// uniq id got from frps, attach it in loginMsg
	runId string


	// manager control connection with server
	ctl   *Control
	ctlMu sync.RWMutex


	pxyCfgs     map[string]config.ProxyConf
	visitorCfgs map[string]config.VisitorConf
	cfgMu       sync.RWMutex


	exit     uint32 // 0 means not exit
	closedCh chan int
}




func NewService(pxyCfgs map[string]config.ProxyConf, visitorCfgs map[string]config.VisitorConf) (svr *Service, err error) {

	// Init assets
	err = assets.Load("")
	if err != nil {
		err = fmt.Errorf("Load assets error: %v", err)
		return
	}

	svr = &Service{
		pxyCfgs:     pxyCfgs,
		visitorCfgs: visitorCfgs,
		exit:        0,
		closedCh:    make(chan int),
	}
	return
}




func (svr *Service) GetController() *Control {
	svr.ctlMu.RLock()
	defer svr.ctlMu.RUnlock()
	return svr.ctl
}





func (svr *Service) Run() error {


	// first login
	for {

		//
		conn, session, err := svr.login()

		if err != nil {
			// login failed


			log.Warn("login to server failed: %v", err)
			// if login_fail_exit is true, just exit this program,
			// otherwise sleep a while and try again to connect to server
			if g.GlbClientCfg.LoginFailExit {
				return err
			} else {
				time.Sleep(10 * time.Second)
			}

		} else {
			// login success


			ctl := NewControl(svr.runId, conn, session, svr.pxyCfgs, svr.visitorCfgs)
			ctl.Run()
			svr.ctlMu.Lock()
			svr.ctl = ctl
			svr.ctlMu.Unlock()
			break
		}
	}

	// 如果意外断连，则重连，重连的核心逻辑和上面的 for 循环里的代码基本一致
	go svr.keepControllerWorking()

	// 是否开启 admin 服务
	if g.GlbClientCfg.AdminPort != 0 {


		err := svr.RunAdminServer(g.GlbClientCfg.AdminAddr, g.GlbClientCfg.AdminPort)
		if err != nil {
			log.Warn("run admin server error: %v", err)
		}
		log.Info("admin server listen on %s:%d", g.GlbClientCfg.AdminAddr, g.GlbClientCfg.AdminPort)
	}

	// 阻塞在 closedCh 管道上
	<-svr.closedCh
	return nil
}




// 如果意外断连，则重连
func (svr *Service) keepControllerWorking() {

	// 控制重连失败的时间阈值
	maxDelayTime := 20 * time.Second

	// 控制重连的退火间隔
	delayTime := time.Second

	for {

		// 阻塞在 ctl.closedDoneCh 管道上，该管道只有可能在 svr.ctl 完全关闭后则会被关闭，（详见 (ctl *Control) worker()  函数）
		// 所以当该管道可读时，可以安全的进行重连和重建 svr.ctl 对象。
		<-svr.ctl.ClosedDoneCh()

		// 判断程序是否已经停止，0 - NotExit
		if atomic.LoadUint32(&svr.exit) != 0 {
			// 非 0 则退出，否则则重连
			return
		}


		for {
			log.Info("try to reconnect to server...")

			// 同 server 间创建控制连接，并注册为 client。
			conn, session, err := svr.login()
			if err != nil {
				// 失败重试，指数退火
				log.Warn("reconnect to server error: %v", err)
				time.Sleep(delayTime)
				delayTime = delayTime * 2
				if delayTime > maxDelayTime {
					delayTime = maxDelayTime
				}
				continue
			}

			// 创建控制连接成功，则重置退火时间基数
			delayTime = time.Second

			// 创建 ctrl 封装控制连接 conn 和一些代理配置
			ctl := NewControl(svr.runId, conn, session, svr.pxyCfgs, svr.visitorCfgs)
			ctl.Run()

			// 将 ctl 赋值给 svr.ctl
			svr.ctlMu.Lock()
			svr.ctl = ctl
			svr.ctlMu.Unlock()
			break
		}
	}
}

// login creates a connection to frps and registers it self as a client.
//
// conn: control connection
// session: if it's not nil, using tcp mux
func (svr *Service) login() (conn frpNet.Conn, session *fmux.Session, err error) {

	var tlsConfig *tls.Config
	if g.GlbClientCfg.TLSEnable {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}
	conn, err = frpNet.ConnectServerByProxyWithTLS(
					g.GlbClientCfg.HttpProxy,
					g.GlbClientCfg.Protocol,
					fmt.Sprintf("%s:%d", g.GlbClientCfg.ServerAddr, g.GlbClientCfg.ServerPort),
					tlsConfig,
				)
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			conn.Close()
			if session != nil {
				session.Close()
			}
		}
	}()


	// tcp 多路复用？
	if g.GlbClientCfg.TcpMux {

		fmuxCfg := fmux.DefaultConfig()
		fmuxCfg.KeepAliveInterval = 20 * time.Second
		fmuxCfg.LogOutput = ioutil.Discard

		session, err = fmux.Client(conn, fmuxCfg)
		if err != nil {
			return
		}

		stream, errRet := session.OpenStream()
		if errRet != nil {
			session.Close()
			err = errRet
			return
		}

		conn = frpNet.WrapConn(stream)
	}

	now := time.Now().Unix()




	// 构造 Login 请求
	loginMsg := &msg.Login{
		Arch:         runtime.GOARCH, 								//服务器架构
		Os:           runtime.GOOS, 								//操作系统
		PoolCount:    g.GlbClientCfg.PoolCount,						//连接池大小
		User:         g.GlbClientCfg.User, 							//用户名
		Version:      version.Full(),								//版本号
		PrivilegeKey: util.GetAuthKey(g.GlbClientCfg.Token, now), 	//加密key
		Timestamp:    now,											//时间戳
		RunId:        svr.runId, 									//runID，首次为空
	}

	// 发送 loginMsg 到 server
	if err = msg.WriteMsg(conn, loginMsg); err != nil {
		return
	}

	// 接受 server 回复的 loginRespMsg
	var loginRespMsg msg.LoginResp
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if err = msg.ReadMsgInto(conn, &loginRespMsg); err != nil {
		return
	}
	conn.SetReadDeadline(time.Time{})
	if loginRespMsg.Error != "" {
		err = fmt.Errorf("%s", loginRespMsg.Error)
		log.Error("%s", loginRespMsg.Error)
		return
	}

	// 得到 loginRespMsg 中的 RunId 并保存到 svr 里
	svr.runId = loginRespMsg.RunId

	// 根据 loginRespMsg 中的 ServerUdpPort 更新全局配置
	g.GlbClientCfg.ServerUdpPort = loginRespMsg.ServerUdpPort
	log.Info("login to server success, get run id [%s], server udp port [%d]", loginRespMsg.RunId, loginRespMsg.ServerUdpPort)
	return
}

func (svr *Service) ReloadConf(pxyCfgs map[string]config.ProxyConf, visitorCfgs map[string]config.VisitorConf) error {
	svr.cfgMu.Lock()
	svr.pxyCfgs = pxyCfgs
	svr.visitorCfgs = visitorCfgs
	svr.cfgMu.Unlock()

	return svr.ctl.ReloadConf(pxyCfgs, visitorCfgs)
}

func (svr *Service) Close() {
	atomic.StoreUint32(&svr.exit, 1)
	svr.ctl.Close()
	close(svr.closedCh)
}
