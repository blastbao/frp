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

package proxy

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"

	"github.com/fatedier/frp/g"
	"github.com/fatedier/frp/models/config"
	"github.com/fatedier/frp/models/msg"
	"github.com/fatedier/frp/server/controller"
	"github.com/fatedier/frp/server/stats"
	"github.com/fatedier/frp/utils/log"
	frpNet "github.com/fatedier/frp/utils/net"

	frpIo "github.com/fatedier/golib/io"
)

type GetWorkConnFn func() (frpNet.Conn, error)

type Proxy interface {
	Run() (remoteAddr string, err error)
	GetName() string
	GetConf() config.ProxyConf
	GetWorkConnFromPool(src, dst net.Addr) (workConn frpNet.Conn, err error)
	GetUsedPortsNum() int
	Close()
	log.Logger
}

type BaseProxy struct {
	name string
	rc   *controller.ResourceController

	statsCollector stats.Collector   // 统计
	listeners      []frpNet.Listener // 监听句柄数组
	usedPortsNum   int               // 占用的端口数目
	poolCount      int               // 预分配的 WorkConn 缓冲池大小
	getWorkConnFn  GetWorkConnFn     // 用于获取 client <-> server 的网络连接 WorkConn

	mu sync.RWMutex
	log.Logger
}

func (pxy *BaseProxy) GetName() string {
	return pxy.name
}

func (pxy *BaseProxy) GetUsedPortsNum() int {
	return pxy.usedPortsNum
}

func (pxy *BaseProxy) Close() {
	pxy.Info("proxy closing")
	for _, l := range pxy.listeners {
		l.Close()
	}
}

// GetWorkConnFromPool try to get a new work connections from pool for quickly response,
// we immediately send the StartWorkConn message to frpc after take out one from pool


//
// 这里 src 是远程用户的 IP，dst 是 frp server 本地的公网 IP
func (pxy *BaseProxy) GetWorkConnFromPool(src, dst net.Addr) (workConn frpNet.Conn, err error) {
	// try all connections from the pool
	for i := 0; i < pxy.poolCount+1; i++ {

		// 1. 调用 pxy.getWorkConnFn() 函数获取一个可用的 client <-> server 连接，详见 (ctl *Control) GetWorkConn() 函数实现。
		if workConn, err = pxy.getWorkConnFn(); err != nil {
			pxy.Warn("failed to get work connection: %v", err)
			return
		}
		pxy.Info("get a new work connection: [%s]", workConn.RemoteAddr().String())

		// 2. 设置日志前缀，添加 proxyName
		workConn.AddLogPrefix(pxy.GetName())

		// 3. 从 net.Addr 中反解出 Src<Addr, Port>, Dest<Addr, Port> 两个 Endpoints
		var (
			srcAddr    string
			dstAddr    string
			srcPortStr string
			dstPortStr string
			srcPort    int
			dstPort    int
		)

		if src != nil {
			srcAddr, srcPortStr, _ = net.SplitHostPort(src.String())
			srcPort, _ = strconv.Atoi(srcPortStr)
		}
		if dst != nil {
			dstAddr, dstPortStr, _ = net.SplitHostPort(dst.String())
			dstPort, _ = strconv.Atoi(dstPortStr)
		}

		// 4. 发送 StartWorkConn 消息给 client, 知会它创建新连接到 server
		err := msg.WriteMsg(workConn, &msg.StartWorkConn{
			ProxyName: pxy.GetName(),
			SrcAddr:   srcAddr, 		// user ip
			SrcPort:   uint16(srcPort), // user port
			DstAddr:   dstAddr,         // server 公网 IP
			DstPort:   uint16(dstPort), // server 公网 PORT
		})

		if err != nil {
			workConn.Warn("failed to send message to work connection from pool: %v, times: %d", err, i)
			workConn.Close()
		} else {
			// 成功则 break 出循环
			break
		}
	}

	if err != nil {
		pxy.Error("try to get work connection failed in the end")
		return
	}

	return
}

// startListenHandler start a goroutine handler for each listener.
//
// p: p will just be passed to handler(Proxy, frpNet.Conn).
// handler: each proxy type can set different handler function to deal with connections accepted from listeners.
func (pxy *BaseProxy) startListenHandler(p Proxy, handler func(Proxy, frpNet.Conn, stats.Collector)) {
	for _, listener := range pxy.listeners {
		go func(l frpNet.Listener) {
			for {
				// block
				// if listener is closed, err returned
				c, err := l.Accept()
				if err != nil {
					pxy.Info("listener is closed")
					return
				}
				pxy.Debug("get a user connection [%s]", c.RemoteAddr().String())
				go handler(p, c, pxy.statsCollector)
			}
		}(listener)
	}
}

func NewProxy(runId string,
	rc *controller.ResourceController,
	statsCollector stats.Collector,
	poolCount int,
	getWorkConnFn GetWorkConnFn,
	pxyConf config.ProxyConf) (pxy Proxy, err error) {

	// 共用相同的 BaseProxy 结构
	basePxy := BaseProxy{
		name:           pxyConf.GetBaseInfo().ProxyName,
		rc:             rc,
		statsCollector: statsCollector,
		listeners:      make([]frpNet.Listener, 0),
		poolCount:      poolCount,
		getWorkConnFn:  getWorkConnFn,
		Logger:         log.NewPrefixLogger(runId),
	}

	// 根据 pxyConf 的类型创建不同的 Proxy 结构
	switch cfg := pxyConf.(type) {
	case *config.TcpProxyConf:
		basePxy.usedPortsNum = 1
		pxy = &TcpProxy{
			BaseProxy: &basePxy,
			cfg:       cfg,
		}
	case *config.HttpProxyConf:
		pxy = &HttpProxy{
			BaseProxy: &basePxy,
			cfg:       cfg,
		}
	case *config.HttpsProxyConf:
		pxy = &HttpsProxy{
			BaseProxy: &basePxy,
			cfg:       cfg,
		}
	case *config.UdpProxyConf:
		basePxy.usedPortsNum = 1
		pxy = &UdpProxy{
			BaseProxy: &basePxy,
			cfg:       cfg,
		}
	case *config.StcpProxyConf:
		pxy = &StcpProxy{
			BaseProxy: &basePxy,
			cfg:       cfg,
		}
	case *config.XtcpProxyConf:
		pxy = &XtcpProxy{
			BaseProxy: &basePxy,
			cfg:       cfg,
		}
	default:
		return pxy, fmt.Errorf("proxy type not support")
	}

	pxy.AddLogPrefix(pxy.GetName())
	return
}



// HandleUserTcpConnection is used for incoming tcp user connections.
// It can be used for tcp, http, https type.
//
// 1. 调用 pxy.GetWorkConnFromPool() 函数从连接池获取一个可用的 frpc <-> frps 的 workConn 连接
// 2. 对 workConn 进行加密、压缩封装
// 3. 在 userConn 和 workConn 直接建立双向连接，阻塞式
// 4. 连接建立、连接关闭、输入流量、输出流量的监控和统计
func HandleUserTcpConnection(pxy Proxy, userConn frpNet.Conn, statsCollector stats.Collector) {
	defer userConn.Close()


	// try all connections from the pool



	// 这里 userConn.RemoteAddr() 是远程用户的 IP，userConn.LocalAddr() 是 server 本地的公网 IP
	workConn, err := pxy.GetWorkConnFromPool(userConn.RemoteAddr(), userConn.LocalAddr())
	if err != nil {
		return
	}
	defer workConn.Close()



	var local io.ReadWriteCloser = workConn
	cfg := pxy.GetConf().GetBaseInfo()


	// 加密？
	if cfg.UseEncryption {
		local, err = frpIo.WithEncryption(local, []byte(g.GlbServerCfg.Token))
		if err != nil {
			pxy.Error("create encryption stream error: %v", err)
			return
		}
	}

	// 压缩？
	if cfg.UseCompression {
		local = frpIo.WithCompression(local)
	}

	pxy.Debug("join connections, workConn(l[%s] r[%s]) userConn(l[%s] r[%s])",
		workConn.LocalAddr().String(),
		workConn.RemoteAddr().String(),
		userConn.LocalAddr().String(),
		userConn.RemoteAddr().String())

	//  监控 - 连接建立
	statsCollector.Mark(stats.TypeOpenConnection,
		&stats.OpenConnectionPayload{
			ProxyName: pxy.GetName(),
		},
	)

	// 在 userConn 和 workConn 直接建立双向连接，阻塞式
	inCount, outCount := frpIo.Join(local, userConn)


	// 监控 - 连接关闭
	statsCollector.Mark(stats.TypeCloseConnection,
		&stats.CloseConnectionPayload{
			ProxyName: pxy.GetName(),
		},
	)

	// 监控 - 输入流量
	statsCollector.Mark(stats.TypeAddTrafficIn,
		&stats.AddTrafficInPayload{
			ProxyName:    pxy.GetName(),
			TrafficBytes: inCount,
		},
	)

	//  监控 - 输出流量
	statsCollector.Mark(stats.TypeAddTrafficOut,
		&stats.AddTrafficOutPayload{
			ProxyName:    pxy.GetName(),
			TrafficBytes: outCount,
		},
	)

	pxy.Debug("join connections closed")
}



// 本质就是个带锁的 Map[proxyName] *proxy
type ProxyManager struct {
	// proxies indexed by proxy name
	pxys map[string]Proxy
	mu sync.RWMutex
}

func NewProxyManager() *ProxyManager {
	return &ProxyManager{
		pxys: make(map[string]Proxy),
	}
}

func (pm *ProxyManager) Add(name string, pxy Proxy) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if _, ok := pm.pxys[name]; ok {
		return fmt.Errorf("proxy name [%s] is already in use", name)
	}
	pm.pxys[name] = pxy
	return nil
}

func (pm *ProxyManager) Del(name string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	delete(pm.pxys, name)
}

func (pm *ProxyManager) GetByName(name string) (pxy Proxy, ok bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	pxy, ok = pm.pxys[name]
	return
}
