// Copyright 2019 fatedier, fatedier@gmail.com
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

	"github.com/fatedier/frp/g"
	"github.com/fatedier/frp/models/config"
	frpNet "github.com/fatedier/frp/utils/net"
)

type TcpProxy struct {
	*BaseProxy
	cfg *config.TcpProxyConf

	realPort int // server 监听的代理端口，该端口上的请求会转发给 client
}

//[1]
// (略)
//[2]
// 1. 根据 client 发来的代理注册消息，确定 server 需要监听的端口号 realPort
// 2. server 根据外网 IP 和监听端口号 realPort 执行监听，也即 Listen(g.GlbServerCfg.ProxyBindAddr, pxy.realPort)，生成 listener 对象
// 3. 对 listener 进行不断循环的 accept + handler 来处理请求。
func (pxy *TcpProxy) Run() (remoteAddr string, err error) {

	// [1]
	if pxy.cfg.Group != "" {

		l, realPort, errRet := pxy.rc.TcpGroupCtl.Listen(pxy.name, pxy.cfg.Group, pxy.cfg.GroupKey, g.GlbServerCfg.ProxyBindAddr, pxy.cfg.RemotePort)
		if errRet != nil {
			err = errRet
			return
		}
		defer func() {
			if err != nil {
				l.Close()
			}
		}()
		pxy.realPort = realPort
		listener := frpNet.WrapLogListener(l)
		listener.AddLogPrefix(pxy.name)
		pxy.listeners = append(pxy.listeners, listener)
		pxy.Info("tcp proxy listen port [%d] in group [%s]", pxy.cfg.RemotePort, pxy.cfg.Group)


	// [2]
	} else {

		// 以 ssh 代理为例，client 发来 msg.NewProxy 消息中的 TcpProxyConf 中的 remote_port 指定了 server 需要监听的端口号，即 remote_port。
		//
		// [ssh]
		// type = tcp
		// local_ip = 127.0.0.1
		// local_port = 22
		// remote_port = 12346

		// server 通过 TcpPortManager 获取 remote_port 端口，并检查其是否空闲且可被监听，若 remote_port 为 0 则随机返回一个可用 port。
		pxy.realPort, err = pxy.rc.TcpPortManager.Acquire(pxy.name, pxy.cfg.RemotePort)
		if err != nil {
			return
		}

		// 如果发生错误则释放 pxy.realPort 端口
		defer func() {
			if err != nil {
				pxy.rc.TcpPortManager.Release(pxy.realPort)
			}
		}()

		// 执行监听 Listen(g.GlbServerCfg.ProxyBindAddr, pxy.realPort)，返回 listener * TcpListener
		listener, errRet := frpNet.ListenTcp(g.GlbServerCfg.ProxyBindAddr, pxy.realPort)
		if errRet != nil {
			err = errRet
			return
		}
		listener.AddLogPrefix(pxy.name)

		// 保存 listener 到 pxy.listeners 中，在后面的 pxy.startListenHandler() 内会对 pxy.listeners 内的
		// 每个 listener 进行不断循环的 accept + handler 来处理请求。
		pxy.listeners = append(pxy.listeners, listener)
		pxy.Info("tcp proxy listen port [%d]", pxy.cfg.RemotePort)
	}

	// 用 pxy.realPort 来重置 pxy.cfg.RemotePort
	pxy.cfg.RemotePort = pxy.realPort
	// 用 pxy.realPort 来生成 remoteAddr，用于返回给 client
	remoteAddr = fmt.Sprintf(":%d", pxy.realPort)
	// 遍历 pxy.listeners[...]，对每个 listener 进行请求监听 accept 和处理 handler
	pxy.startListenHandler(pxy, HandleUserTcpConnection)
	return
}

func (pxy *TcpProxy) GetConf() config.ProxyConf {
	return pxy.cfg
}

func (pxy *TcpProxy) Close() {
	pxy.BaseProxy.Close()
	if pxy.cfg.Group == "" {
		pxy.rc.TcpPortManager.Release(pxy.realPort)
	}
}
