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
	"io"
	"runtime/debug"
	"sync"
	"time"

	"github.com/fatedier/frp/client/proxy"
	"github.com/fatedier/frp/g"
	"github.com/fatedier/frp/models/config"
	"github.com/fatedier/frp/models/msg"
	"github.com/fatedier/frp/utils/log"
	frpNet "github.com/fatedier/frp/utils/net"

	"github.com/fatedier/golib/control/shutdown"
	"github.com/fatedier/golib/crypto"
	fmux "github.com/hashicorp/yamux"
)

type Control struct {
	// uniq id got from frps, attach it in loginMsg
	// 标识 id，在 loginMsg 中会附带它
	runId string

	// manage all proxies
	pxyCfgs map[string]config.ProxyConf
	pm      *proxy.ProxyManager

	// manage all visitors
	vm *VisitorManager

	// control connection
	//
	conn frpNet.Conn

	// tcp stream multiplexing, if enabled
	// tcp 多路复用
	session *fmux.Session

	// put a message in this channel to send it over control connection to server
	sendCh chan (msg.Message)

	// read from this channel to get the next message sent by server
	readCh chan (msg.Message)

	// goroutines can block by reading from this channel, it will be closed only in reader() when control connection is closed
	// goroutines 可以从 closedCh 管道读取停止指令，当 reader() 中 connection 被关闭时，会主动 close(closedCh) 关闭管道。
	closedCh chan struct{}

	closedDoneCh chan struct{}

	// last time got the Pong message
	lastPong time.Time

	readerShutdown     *shutdown.Shutdown
	writerShutdown     *shutdown.Shutdown
	msgHandlerShutdown *shutdown.Shutdown

	mu sync.RWMutex

	log.Logger
}

func NewControl(runId string, conn frpNet.Conn, session *fmux.Session, pxyCfgs map[string]config.ProxyConf, visitorCfgs map[string]config.VisitorConf) *Control {
	ctl := &Control{
		runId:              runId,
		conn:               conn,
		session:            session,
		pxyCfgs:            pxyCfgs,
		sendCh:             make(chan msg.Message, 100),
		readCh:             make(chan msg.Message, 100),
		closedCh:           make(chan struct{}),
		closedDoneCh:       make(chan struct{}),
		readerShutdown:     shutdown.New(),
		writerShutdown:     shutdown.New(),
		msgHandlerShutdown: shutdown.New(),
		Logger:             log.NewPrefixLogger(""),
	}

	//
	ctl.pm = proxy.NewProxyManager(ctl.sendCh, runId)

	ctl.vm = NewVisitorManager(ctl)

	ctl.vm.Reload(visitorCfgs)

	return ctl
}

func (ctl *Control) Run() {
	go ctl.worker()

	// start all proxies
	ctl.pm.Reload(ctl.pxyCfgs)

	// start all visitors
	go ctl.vm.Run()
	return
}






func (ctl *Control) HandleReqWorkConn(inMsg *msg.ReqWorkConn) {

	// 同 server 建立新连接 workConn
	workConn, err := ctl.connectServer()
	if err != nil {
		return
	}

	m := &msg.NewWorkConn{
		RunId: ctl.runId,
	}

	// 通过新连接 workConn 给 server 发送消息
	if err = msg.WriteMsg(workConn, m); err != nil {
		ctl.Warn("work connection write to server error: %v", err)
		workConn.Close()
		return
	}

	// 等待 server 回复 StartWorkConn 消息
	var startMsg msg.StartWorkConn
	if err = msg.ReadMsgInto(workConn, &startMsg); err != nil {
		ctl.Error("work connection closed, %v", err)
		workConn.Close()
		return
	}
	workConn.AddLogPrefix(startMsg.ProxyName)


	// dispatch this work connection to related proxy
	ctl.pm.HandleWorkConn(startMsg.ProxyName, workConn, &startMsg)
}





func (ctl *Control) HandleNewProxyResp(inMsg *msg.NewProxyResp) {
	// Server will return NewProxyResp message to each NewProxy message.
	// Start a new proxy handler if no error got
	err := ctl.pm.StartProxy(inMsg.ProxyName, inMsg.RemoteAddr, inMsg.Error)
	if err != nil {
		ctl.Warn("[%s] start error: %v", inMsg.ProxyName, err)
	} else {
		ctl.Info("[%s] start proxy success", inMsg.ProxyName)
	}
}

func (ctl *Control) Close() error {
	ctl.pm.Close()
	ctl.conn.Close()
	if ctl.session != nil {
		ctl.session.Close()
	}
	return nil
}

// ClosedDoneCh returns a channel which will be closed after all resources are released
func (ctl *Control) ClosedDoneCh() <-chan struct{} {
	return ctl.closedDoneCh
}




// connectServer return a new connection to frps
func (ctl *Control) connectServer() (conn frpNet.Conn, err error) {


	if g.GlbClientCfg.TcpMux {

		stream, errRet := ctl.session.OpenStream()
		if errRet != nil {
			err = errRet
			ctl.Warn("start new connection to server error: %v", err)
			return
		}
		conn = frpNet.WrapConn(stream)

	} else {

		// 构造 tls 配置结构体
		var tlsConfig *tls.Config
		if g.GlbClientCfg.TLSEnable {
			tlsConfig = &tls.Config{
				InsecureSkipVerify: true,
			}
		}

		//
		conn, err = frpNet.ConnectServerByProxyWithTLS(
			g.GlbClientCfg.HttpProxy,
			g.GlbClientCfg.Protocol,
			fmt.Sprintf("%s:%d", g.GlbClientCfg.ServerAddr, g.GlbClientCfg.ServerPort),
			tlsConfig,
		)

		if err != nil {
			ctl.Warn("start new connection to server error: %v", err)
			return
		}

	}
	return
}







// reader read all messages from frps and send to readCh
func (ctl *Control) reader() {
	defer func() {
		if err := recover(); err != nil {
			ctl.Error("panic error: %v", err)
			ctl.Error(string(debug.Stack()))
		}
	}()
	defer ctl.readerShutdown.Done()
	defer close(ctl.closedCh)

	encReader := crypto.NewReader(ctl.conn, []byte(g.GlbClientCfg.Token))
	for {
		if m, err := msg.ReadMsg(encReader); err != nil {
			if err == io.EOF {
				ctl.Debug("read from control connection EOF")
				return
			} else {
				ctl.Warn("read error: %v", err)
				ctl.conn.Close()
				return
			}
		} else {
			ctl.readCh <- m
		}
	}
}

// writer writes messages got from sendCh to frps
func (ctl *Control) writer() {
	defer ctl.writerShutdown.Done()
	encWriter, err := crypto.NewWriter(ctl.conn, []byte(g.GlbClientCfg.Token))
	if err != nil {
		ctl.conn.Error("crypto new writer error: %v", err)
		ctl.conn.Close()
		return
	}
	for {
		if m, ok := <-ctl.sendCh; !ok {
			ctl.Info("control writer is closing")
			return
		} else {
			if err := msg.WriteMsg(encWriter, m); err != nil {
				ctl.Warn("write message to control connection error: %v", err)
				return
			}
		}
	}
}



// msgHandler handles all channel events and do corresponding operations.
//
// 本函数主要处理 client 和 server 之间的消息交互。
//
// 1. 设置 Ping 发送间隔、Pong 检测间隔
// 2. 定时发送 Ping 心跳消息
// 3. 定时检查 Pong 回复消息
// 4. 接受 server 发来的控制消息，主要是 ReqWorkConn、NewProxyResp、Pong
func (ctl *Control) msgHandler() {

	defer func() {
		if err := recover(); err != nil {
			ctl.Error("panic error: %v", err)
			ctl.Error(string(debug.Stack()))
		}
	}()
	defer ctl.msgHandlerShutdown.Done()

	// 设置 Ping 发送间隔
	hbSend := time.NewTicker(time.Duration(g.GlbClientCfg.HeartBeatInterval) * time.Second)
	defer hbSend.Stop()

	// 设置 Pong 检测间隔
	hbCheck := time.NewTicker(time.Second)
	defer hbCheck.Stop()

	// 设置收到上一个 Pong 的时间，用于判断心跳超时
	ctl.lastPong = time.Now()

	for {
		select {

		// 发送 Ping 心跳
		case <-hbSend.C:
			ctl.Debug("send heartbeat to server")
			ctl.sendCh <- &msg.Ping{}

		// 检测 Pong 回应
		case <-hbCheck.C:

			// 如果上个 Pong 接受时间距现在超过一个心跳的间隔，意味着在一个心跳窗口内为收到服务器回应，则关闭连接。
			if time.Since(ctl.lastPong) > time.Duration(g.GlbClientCfg.HeartBeatTimeout)*time.Second {
				ctl.Warn("heartbeat timeout")
				// let reader() stop
				ctl.conn.Close()
				return
			}

		// 读取 server 发来的消息
		case rawMsg, ok := <-ctl.readCh:

			if !ok {
				return
			}

			switch m := rawMsg.(type) {

			// 创建 workConn 消息:
			case *msg.ReqWorkConn:
				go ctl.HandleReqWorkConn(m)
			// NewProxy 的确认消息
			case *msg.NewProxyResp:
				ctl.HandleNewProxyResp(m)
			// 心跳回应 Pong 消息
			case *msg.Pong:
				ctl.lastPong = time.Now() // 更新 Pong 接收时间
				ctl.Debug("receive heartbeat from server")
			}
		}
	}
}





// If controler is notified by closedCh, reader and writer and handler will exit
//
//
// 主函数: 消息读取、消息处理、消息回复、监听关闭信号，完成清理工作。
func (ctl *Control) worker() {

	go ctl.msgHandler()  	// 消息处理: 1. 接收 server 发来的消息并进行相应处理；2. 发送心跳消息给 server。
	go ctl.reader() 		// 将 server 发来的消息从 ctl.conn 读到 ctl.readCh
	go ctl.writer()			// 将 ctl.sendCh 中的消息写入到同 server 的连接 ctl.conn 中

	select {

	// 阻塞到 ctl.closedCh 管道上等待退出指令
	case <-ctl.closedCh:

		// close related channels and wait until other goroutines done


		close(ctl.readCh)
		ctl.readerShutdown.WaitDone()
		ctl.msgHandlerShutdown.WaitDone()

		close(ctl.sendCh)
		ctl.writerShutdown.WaitDone()


		ctl.pm.Close()
		ctl.vm.Close()


		// 完成清理工作后，关闭 ctl.closedDoneCh 进行广播通知，可能会触发 svr.keepControllerWorking() 中进行重连。
		close(ctl.closedDoneCh)


		if ctl.session != nil {
			ctl.session.Close()
		}
		return
	}
}

func (ctl *Control) ReloadConf(pxyCfgs map[string]config.ProxyConf, visitorCfgs map[string]config.VisitorConf) error {
	ctl.vm.Reload(visitorCfgs)
	ctl.pm.Reload(pxyCfgs)
	return nil
}
