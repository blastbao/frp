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

package server

import (
	"fmt"
	"io"
	"runtime/debug"
	"sync"
	"time"

	"github.com/fatedier/frp/g"
	"github.com/fatedier/frp/models/config"
	"github.com/fatedier/frp/models/consts"
	frpErr "github.com/fatedier/frp/models/errors"
	"github.com/fatedier/frp/models/msg"
	"github.com/fatedier/frp/server/controller"
	"github.com/fatedier/frp/server/proxy"
	"github.com/fatedier/frp/server/stats"
	"github.com/fatedier/frp/utils/net"
	"github.com/fatedier/frp/utils/version"

	"github.com/fatedier/golib/control/shutdown"
	"github.com/fatedier/golib/crypto"
	"github.com/fatedier/golib/errors"
)

type ControlManager struct {
	// controls indexed by run id
	ctlsByRunId map[string]*Control

	mu sync.RWMutex
}

func NewControlManager() *ControlManager {
	return &ControlManager{
		ctlsByRunId: make(map[string]*Control),
	}
}

func (cm *ControlManager) Add(runId string, ctl *Control) (oldCtl *Control) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	oldCtl, ok := cm.ctlsByRunId[runId]
	if ok {
		oldCtl.Replaced(ctl)
	}
	cm.ctlsByRunId[runId] = ctl
	return
}

// we should make sure if it's the same control to prevent delete a new one
func (cm *ControlManager) Del(runId string, ctl *Control) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if c, ok := cm.ctlsByRunId[runId]; ok && c == ctl {
		delete(cm.ctlsByRunId, runId)
	}
}

func (cm *ControlManager) GetById(runId string) (ctl *Control, ok bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	ctl, ok = cm.ctlsByRunId[runId]
	return
}



// Server 的控制 control
type Control struct {

	// all resource managers and controllers
	rc *controller.ResourceController

	// proxy manager
	pxyManager *proxy.ProxyManager

	// stats collector to store stats info of clients and proxies
	statsCollector stats.Collector


	// login message
	// 登录消息
	loginMsg *msg.Login

	// control connection
	// 网络连接
	conn net.Conn

	// put a message in this channel to send it over control connection to client
	// 发送队列，发消息给 client
	sendCh chan (msg.Message)

	// read from this channel to get the next message sent by client
	// 接收队列，读取 client 发来的消息
	readCh chan (msg.Message)

	// work connections
	workConnCh chan net.Conn

	// proxies in one client
	proxies map[string]proxy.Proxy

	// pool count
	poolCount int

	// ports used, for limitations
	portsUsedNum int

	// last time got the Ping message
	lastPing time.Time

	// A new run id will be generated when a new client login.
	// If run id got from login message has same run id, it means it's the same client, so we can
	// replace old controller instantly.
	runId string

	// control status
	status string

	readerShutdown  *shutdown.Shutdown
	writerShutdown  *shutdown.Shutdown
	managerShutdown *shutdown.Shutdown
	allShutdown     *shutdown.Shutdown

	mu sync.RWMutex
}

func NewControl(rc *controller.ResourceController,
				pxyManager *proxy.ProxyManager,
				statsCollector stats.Collector,
				ctlConn net.Conn,
				loginMsg *msg.Login) *Control {

	return &Control{
		rc:              rc,
		pxyManager:      pxyManager,
		statsCollector:  statsCollector,
		conn:            ctlConn,
		loginMsg:        loginMsg,
		sendCh:          make(chan msg.Message, 10),   	// 发送长度10
		readCh:          make(chan msg.Message, 10),	// 读长度 10
		workConnCh:      make(chan net.Conn, loginMsg.PoolCount+10),	//workconnection 通道,
		proxies:         make(map[string]proxy.Proxy), 	// 代理数组
		poolCount:       loginMsg.PoolCount, 			// pool 数
		portsUsedNum:    0,
		lastPing:        time.Now(),
		runId:           loginMsg.RunId, 				// 拿到loginMsg中的runid
		status:          consts.Working, 				// 状态
		readerShutdown:  shutdown.New(),
		writerShutdown:  shutdown.New(),
		managerShutdown: shutdown.New(),
		allShutdown:     shutdown.New(),
	}
}





// Start send a login success message to client and start working.


// 1. 向 client 发送一个 LoginResp 的消息，告知 client 其 Login 成功
// 2. 启动 ctl.writer()，不断从 ctl.sendCh 中读取消息并发送给 client
// 3. 初始化 workConn 连接池，连续向 client 发送多个 msg.ReqWorkConn{} 消息，让 client 建立多个 conn 到 server
// 4. 启动 ctl.manager()，主要行为是:
// 		（1）心跳检测：检查 client 是否在正常发送 ping 消息，若不正常则关闭连接；
// 		（2）从 ctl.readCh 读取消息、处理、写回响应消息到 ctl.sendCh。
//	  可见，ctl.manager() 可被视为  ctl.writer() 和 ctl.reader() 之间的桥梁。
// 5. ctl.reader()，不断从和 client 的网络连接中读取消息并解析，然后发送到 ctl.readCh，然后 ctl.manager() 会读取出该消息并处理。
// 6. ctl.stoper()，


func (ctl *Control) Start() {


	// 向 client 发送一个 LoginResp 的消息，告知 client 成功
	loginRespMsg := &msg.LoginResp{
		Version:       version.Full(),
		RunId:         ctl.runId,
		ServerUdpPort: g.GlbServerCfg.BindUdpPort,
		Error:         "",
	}

	// 数据写入到 connection
	msg.WriteMsg(ctl.conn, loginRespMsg)

	// 从 ctl.sendCh 中读取消息并发送给客户端
	go ctl.writer()

	// 与客户端开启多个连接
	for i := 0; i < ctl.poolCount; i++ {
		ctl.sendCh <- &msg.ReqWorkConn{}
	}

	// 1. 心跳检查与发起重连
	// 2. 从 ctl.readCh 中读取消息
	go ctl.manager()

	// 将从 socket 中读取消息并解析，然后发送到 ctl.readCh
	go ctl.reader()

	//
	go ctl.stoper()
}


// 把 conn 放到 workConnCh 中，如果已经放满，就 close 掉。
func (ctl *Control) RegisterWorkConn(conn net.Conn) {
	defer func() {
		if err := recover(); err != nil {
			ctl.conn.Error("panic error: %v", err)
			ctl.conn.Error(string(debug.Stack()))
		}
	}()
	select {
	case ctl.workConnCh <- conn:
		ctl.conn.Debug("new work connection registered")
	default:
		ctl.conn.Debug("work connection pool is full, discarding")
		conn.Close()
	}
}




// When frps get one user connection, we get one work connection from the pool and return it.
// If no workConn available in the pool, send message to frpc to get one or more and wait until it is available.
// return an error if wait timeout
func (ctl *Control) GetWorkConn() (workConn net.Conn, err error) {


	// 这块设计思路是酱紫：因为 client 并不是一个服务，所以 server 不能主动和 client 建立连接，
	// 但是当有新的公网 user 连接请求到达时，为能支持请求转发，需要一条独立的 client <-> server 连接，
	// 如果此时 client 和 server 间可用连接不足，则 server 主动通过控制信道通知 client 建立新的
	// 连接到 server 上，这样这个新链接就可以被 server 分配给新的 user 来使用。


	defer func() {
		if err := recover(); err != nil {
			ctl.conn.Error("panic error: %v", err)
			ctl.conn.Error(string(debug.Stack()))
		}
	}()

	var ok bool
	select {

	// get a work connection from the pool
	// 从 workConnCh 中取出一个 workConn
	case workConn, ok = <-ctl.workConnCh:
		if !ok {
			err = frpErr.ErrCtlClosed
			return
		}
		ctl.conn.Debug("get work connection from pool")

	// no work connections available in the poll, send message to frpc to get more
	// 若无可用连接，就告知 client 创建新连接来使用
	default:

		// 1. 发消息给 client 请求新 workConn
		err = errors.PanicToError(func() {
			ctl.sendCh <- &msg.ReqWorkConn{}
		})
		if err != nil {
			ctl.conn.Error("%v", err)
			return
		}

		// 2. 等待可用 workConn, 带超时
		select {
		case workConn, ok = <-ctl.workConnCh:
			if !ok {
				err = frpErr.ErrCtlClosed
				ctl.conn.Warn("no work connections avaiable, %v", err)
				return
			}
		case <-time.After(time.Duration(g.GlbServerCfg.UserConnTimeout) * time.Second):
			err = fmt.Errorf("timeout trying to get work connection")
			ctl.conn.Warn("%v", err)
			return
		}
	}


	// When we get a work connection from pool, replace it with a new one.
	// 如果我们从 connPool 中取出了一个连接，则通知 client 补充一个新连接
	errors.PanicToError(func() {
		ctl.sendCh <- &msg.ReqWorkConn{}
	})

	return
}

func (ctl *Control) Replaced(newCtl *Control) {
	ctl.conn.Info("Replaced by client [%s]", newCtl.runId)
	ctl.runId = ""
	ctl.allShutdown.Start()
}

func (ctl *Control) writer() {

	defer func() {
		// panic 捕获
		if err := recover(); err != nil {
			ctl.conn.Error("panic error: %v", err) //记录日志
			ctl.conn.Error(string(debug.Stack()))  //记录日志
		}
	}()

	defer ctl.allShutdown.Start()
	defer ctl.writerShutdown.Done()

	// 构造加密写
	encWriter, err := crypto.NewWriter(ctl.conn, []byte(g.GlbServerCfg.Token))
	if err != nil {
		ctl.conn.Error("crypto new writer error: %v", err)
		ctl.allShutdown.Start()
		return
	}

	// 不断从 ctl.sendCh 读取消息，然后以加密方式写入到同 client 的连接 conn 中，直到 ctl.sendCh 被关闭。
	for {
		if m, ok := <-ctl.sendCh; !ok {
			ctl.conn.Info("control writer is closing")
			return
		} else {
			if err := msg.WriteMsg(encWriter, m); err != nil {
				ctl.conn.Warn("write message to control connection error: %v", err)
				return
			}
		}
	}
}


func (ctl *Control) reader() {
	defer func() {
		if err := recover(); err != nil {
			ctl.conn.Error("panic error: %v", err)
			ctl.conn.Error(string(debug.Stack()))
		}
	}()

	// ctrl 的清理工作
	defer ctl.allShutdown.Start()
	// ctrl 的清理完成
	defer ctl.readerShutdown.Done()

	// 构造解密读
	encReader := crypto.NewReader(ctl.conn, []byte(g.GlbServerCfg.Token))

	// 不断从 client 的连接 conn 中读取数据，解析成 msg 后写入到 ctl.readCh 中，直到 conn 被关闭。
	for {
		if m, err := msg.ReadMsg(encReader); err != nil {

			// EOF 表示 ctl.conn 关闭
			if err == io.EOF {
				ctl.conn.Debug("control connection closed")
				return
			} else {
				ctl.conn.Warn("read error: %v", err)
				ctl.conn.Close()
				return
			}
		} else {
			// 将 msg 放到 readCh
			ctl.readCh <- m
		}
	}
}



func (ctl *Control) stoper() {
	defer func() {
		if err := recover(); err != nil {
			ctl.conn.Error("panic error: %v", err)
			ctl.conn.Error(string(debug.Stack()))
		}
	}()

	ctl.allShutdown.WaitStart()

	close(ctl.readCh)
	ctl.managerShutdown.WaitDone()

	close(ctl.sendCh)
	ctl.writerShutdown.WaitDone()

	ctl.conn.Close()
	ctl.readerShutdown.WaitDone()

	ctl.mu.Lock()
	defer ctl.mu.Unlock()

	close(ctl.workConnCh)
	for workConn := range ctl.workConnCh {
		workConn.Close()
	}

	for _, pxy := range ctl.proxies {
		pxy.Close()
		ctl.pxyManager.Del(pxy.GetName())
		ctl.statsCollector.Mark(stats.TypeCloseProxy, &stats.CloseProxyPayload{
			Name:      pxy.GetName(),
			ProxyType: pxy.GetConf().GetBaseInfo().ProxyType,
		})
	}

	ctl.allShutdown.Done()
	ctl.conn.Info("client exit success")

	ctl.statsCollector.Mark(stats.TypeCloseClient, &stats.CloseClientPayload{})
}



// block until Control closed
func (ctl *Control) WaitClosed() {
	ctl.allShutdown.WaitDone()
}

// 当 client 和 server 都启动，并且 client 成功 login 之后，
// client 会发送 NewProxy、CloseProxy、Ping 等消息给 server 。
// 而 server 收到 msg.NewProxy 消息后，便会创建和初始化对应的 Proxy 对象，如 TcpProxy 等。
func (ctl *Control) manager() {
	defer func() {
		if err := recover(); err != nil {
			ctl.conn.Error("panic error: %v", err)
			ctl.conn.Error(string(debug.Stack()))
		}
	}()

	defer ctl.allShutdown.Start()
	defer ctl.managerShutdown.Done()

	heartbeat := time.NewTicker(time.Second)
	defer heartbeat.Stop()

	for {
		select {

		// 心跳检测：检查 client 是否在正常发送 ping 消息
		case <-heartbeat.C:
			// 一个心跳窗口内没有收到 client 发来的 ping，则认为它已经挂了，报错。
			if time.Since(ctl.lastPing) > time.Duration(g.GlbServerCfg.HeartBeatTimeout)*time.Second {
				ctl.conn.Warn("heartbeat timeout")
				return
			}

		// 读取 client 发送的消息
		case rawMsg, ok := <-ctl.readCh:
			if !ok {
				return
			}

			switch m := rawMsg.(type) {

			// 收到 NewProxy 消息
			case *msg.NewProxy:

				// 根据 NewProxy 消息所含的代理配置信息创建 Proxy，将其注册到 ctl 并启动 [监听&代理转发]，返回其在 server 上的代理监听地址 remoteAddr 。
				remoteAddr, err := ctl.RegisterProxy(m) // register proxy in this control

				// 构造回复消息
				resp := &msg.NewProxyResp{
					ProxyName: m.ProxyName,
				}

				// 创建成功 or 失败
				if err != nil {
					// 设置错误信息
					resp.Error = err.Error()
					ctl.conn.Warn("new proxy [%s] error: %v", m.ProxyName, err)
				} else {
					// 设置当前代理 proxy 在 server 上的监听地址 "addr:port" 。
					resp.RemoteAddr = remoteAddr
					ctl.conn.Info("new proxy [%s] success", m.ProxyName)
					// 监控上报 - 创建 Proxy
					ctl.statsCollector.Mark(
						stats.TypeNewProxy,
						&stats.NewProxyPayload{
							Name:      m.ProxyName,
							ProxyType: m.ProxyType,
						},
					)
				}

				// 将消息resp * NewProxyResp 放到回复队列中，等待发给 client
				ctl.sendCh <- resp

			case *msg.CloseProxy:
				// 关闭 proxy
				ctl.CloseProxy(m)
				ctl.conn.Info("close proxy [%s] success", m.ProxyName)
			case *msg.Ping:
				// 收到 client 心跳 Ping ，更新心跳接收时间，然后回复 Pong 心跳响应包
				ctl.lastPing = time.Now()
				ctl.conn.Debug("receive heartbeat")
				ctl.sendCh <- &msg.Pong{}
			}
		}
	}
}





// 1. 根据 client 发来的 pxyMsg 消息来生成特定类型的 pxyConf 配置
// 2. 根据 pxyConf 配置的类型创建不同的 pxy 对象，比如 ssh 代理会返回 TcpProxy
// 3. 检查 ctrl 占用的总端口数目 ctl.portsUsedNum 是否超过阈值；更新 ctl.portsUsedNum；
// 4. 启动 pxy.Run()，
// 5. 保存 pair<pxyName, pxy> 到 ctl.pxyManager 中
// 6. 保存 pair<pxyName, pxy> 到 ctl.proxies 中
func (ctl *Control) RegisterProxy(pxyMsg *msg.NewProxy) (remoteAddr string, err error) {
	var pxyConf config.ProxyConf

	// Load configures from NewProxy message and check.
	//
	// 根据 pxyMsg 消息来生成特定协议的 pxyConf 配置结构体对象
	pxyConf, err = config.NewProxyConfFromMsg(pxyMsg)
	if err != nil {
		return
	}

	// NewProxy will return a interface Proxy.
	// In fact it create different proxies by different proxy type, we just call run() here.
	//
	// 根据 pxyConf 的类型创建不同的 Proxy 对象，比如 ssh 代理会返回 TcpProxy
	pxy, err := proxy.NewProxy(ctl.runId, ctl.rc, ctl.statsCollector, ctl.poolCount, ctl.GetWorkConn, pxyConf)
	if err != nil {
		return remoteAddr, err
	}



	// Check ports used number in each client
	//
	// 如果设置了单个 client 所能开启的最多端口数
	if g.GlbServerCfg.MaxPortsPerClient > 0 {

		ctl.mu.Lock()
		// 检查当前 ctrl 占用的总端口号 和 新 proxy 占用的端口号 的总数是否超过阈值，若超过则报错
		if ctl.portsUsedNum + pxy.GetUsedPortsNum() > int(g.GlbServerCfg.MaxPortsPerClient) {
			ctl.mu.Unlock()
			err = fmt.Errorf("exceed the max_ports_per_client")
			return
		}

		// 更新当前 ctrl 占用的总端口号的总数
		ctl.portsUsedNum = ctl.portsUsedNum + pxy.GetUsedPortsNum()
		ctl.mu.Unlock()

		// 如果函数出错，则在退出时更新 ctl.portsUsedNum
		defer func() {
			if err != nil {
				ctl.mu.Lock()
				// 出错则减回来
				ctl.portsUsedNum = ctl.portsUsedNum - pxy.GetUsedPortsNum()
				ctl.mu.Unlock()
			}
		}()
	}

	// [重要]
	// 运行 pxy.Run()，它返回 server 监听的代理地址 remoteAddr = addr:port
	remoteAddr, err = pxy.Run()
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			pxy.Close()
		}
	}()

	// 保存 pxyName => pxy 的映射到 ctl.pxyManager 中
	err = ctl.pxyManager.Add(pxyMsg.ProxyName, pxy)
	if err != nil {
		return
	}

	// 加入到 ctl.proxies 中
	ctl.mu.Lock()
	ctl.proxies[pxy.GetName()] = pxy
	ctl.mu.Unlock()

	return
}

func (ctl *Control) CloseProxy(closeMsg *msg.CloseProxy) (err error) {
	ctl.mu.Lock()
	pxy, ok := ctl.proxies[closeMsg.ProxyName]
	if !ok {
		ctl.mu.Unlock()
		return
	}

	if g.GlbServerCfg.MaxPortsPerClient > 0 {
		ctl.portsUsedNum = ctl.portsUsedNum - pxy.GetUsedPortsNum()
	}
	pxy.Close()
	ctl.pxyManager.Del(pxy.GetName())
	delete(ctl.proxies, closeMsg.ProxyName)
	ctl.mu.Unlock()

	ctl.statsCollector.Mark(stats.TypeCloseProxy, &stats.CloseProxyPayload{
		Name:      pxy.GetName(),
		ProxyType: pxy.GetConf().GetBaseInfo().ProxyType,
	})
	return
}
