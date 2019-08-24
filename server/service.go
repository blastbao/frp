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
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"math/big"
	"net"
	"net/http"
	"time"

	"github.com/fatedier/frp/assets"
	"github.com/fatedier/frp/g"
	"github.com/fatedier/frp/models/msg"
	"github.com/fatedier/frp/models/nathole"
	"github.com/fatedier/frp/server/controller"
	"github.com/fatedier/frp/server/group"
	"github.com/fatedier/frp/server/ports"
	"github.com/fatedier/frp/server/proxy"
	"github.com/fatedier/frp/server/stats"
	"github.com/fatedier/frp/utils/log"
	frpNet "github.com/fatedier/frp/utils/net"
	"github.com/fatedier/frp/utils/util"
	"github.com/fatedier/frp/utils/version"
	"github.com/fatedier/frp/utils/vhost"

	"github.com/fatedier/golib/net/mux"
	fmux "github.com/hashicorp/yamux"
)

const (
	connReadTimeout time.Duration = 10 * time.Second
)

var ServerService *Service

// Server service
type Service struct {
	// Dispatch connections to different handlers listen on same port
	muxer *mux.Mux

	// Accept connections from client
	listener frpNet.Listener

	// Accept connections using kcp
	kcpListener frpNet.Listener

	// Accept connections using websocket
	websocketListener frpNet.Listener

	// Accept frp tls connections
	tlsListener frpNet.Listener

	// Manage all controllers
	ctlManager *ControlManager

	// Manage all proxies
	pxyManager *proxy.ProxyManager

	// HTTP vhost router
	httpVhostRouter *vhost.VhostRouters

	// All resource managers and controllers
	rc *controller.ResourceController

	// stats collector to store server and proxies stats info
	statsCollector stats.Collector

	tlsConfig *tls.Config
}

func NewService() (svr *Service, err error) {

	cfg := &g.GlbServerCfg.ServerCommonConf

	svr = &Service{
		ctlManager: NewControlManager(),      // 新建ctl-Manager
		pxyManager: proxy.NewProxyManager(),  // 新建Proxy-Manager

		rc: &controller.ResourceController{
			VisitorManager: controller.NewVisitorManager(),

			TcpPortManager: ports.NewPortManager("tcp", cfg.ProxyBindAddr, cfg.AllowPorts),
			UdpPortManager: ports.NewPortManager("udp", cfg.ProxyBindAddr, cfg.AllowPorts),
		},

		httpVhostRouter: vhost.NewVhostRouters(),
		tlsConfig:       generateTLSConfig(),
	}

	// Init group controller
	svr.rc.TcpGroupCtl = group.NewTcpGroupCtl(svr.rc.TcpPortManager)

	// Init HTTP group controller
	svr.rc.HTTPGroupCtl = group.NewHTTPGroupController(svr.httpVhostRouter)


	// Init assets
	// 资源文件加载
	err = assets.Load(cfg.AssetsDir)
	if err != nil {
		err = fmt.Errorf("Load assets error: %v", err)
		return
	}

	// Init 404 not found page
	vhost.NotFoundPagePath = cfg.Custom404Page

	var (
		httpMuxOn  bool
		httpsMuxOn bool
	)
	if cfg.BindAddr == cfg.ProxyBindAddr {
		if cfg.BindPort == cfg.VhostHttpPort {
			httpMuxOn = true
		}
		if cfg.BindPort == cfg.VhostHttpsPort {
			httpsMuxOn = true
		}
	}

	// Listen for accepting connections from client.
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", cfg.BindAddr, cfg.BindPort))
	if err != nil {
		err = fmt.Errorf("Create server listener error, %v", err)
		return
	}

	svr.muxer = mux.NewMux(ln)
	go svr.muxer.Serve()
	ln = svr.muxer.DefaultListener()

	svr.listener = frpNet.WrapLogListener(ln)
	log.Info("frps tcp listen on %s:%d", cfg.BindAddr, cfg.BindPort)

	// Listen for accepting connections from client using kcp protocol.
	if cfg.KcpBindPort > 0 {
		svr.kcpListener, err = frpNet.ListenKcp(cfg.BindAddr, cfg.KcpBindPort)
		if err != nil {
			err = fmt.Errorf("Listen on kcp address udp [%s:%d] error: %v", cfg.BindAddr, cfg.KcpBindPort, err)
			return
		}
		log.Info("frps kcp listen on udp %s:%d", cfg.BindAddr, cfg.KcpBindPort)
	}

	// Listen for accepting connections from client using websocket protocol.
	websocketPrefix := []byte("GET " + frpNet.FrpWebsocketPath)
	websocketLn := svr.muxer.Listen(0, uint32(len(websocketPrefix)), func(data []byte) bool {
		return bytes.Equal(data, websocketPrefix)
	})
	svr.websocketListener = frpNet.NewWebsocketListener(websocketLn)

	// Create http vhost muxer.
	if cfg.VhostHttpPort > 0 {

		rp := vhost.NewHttpReverseProxy(
			vhost.HttpReverseProxyOptions{
				ResponseHeaderTimeoutS: cfg.VhostHttpTimeout,
			},
			svr.httpVhostRouter,
		)
		svr.rc.HttpReverseProxy = rp

		address := fmt.Sprintf("%s:%d", cfg.ProxyBindAddr, cfg.VhostHttpPort)
		server := &http.Server{
			Addr:    address,
			Handler: rp,
		}
		var l net.Listener
		if httpMuxOn {
			l = svr.muxer.ListenHttp(1)
		} else {
			l, err = net.Listen("tcp", address)
			if err != nil {
				err = fmt.Errorf("Create vhost http listener error, %v", err)
				return
			}
		}
		go server.Serve(l)
		log.Info("http service listen on %s:%d", cfg.ProxyBindAddr, cfg.VhostHttpPort)
	}

	// Create https vhost muxer.
	if cfg.VhostHttpsPort > 0 {
		var l net.Listener
		if httpsMuxOn {
			l = svr.muxer.ListenHttps(1)
		} else {
			l, err = net.Listen("tcp", fmt.Sprintf("%s:%d", cfg.ProxyBindAddr, cfg.VhostHttpsPort))
			if err != nil {
				err = fmt.Errorf("Create server listener error, %v", err)
				return
			}
		}

		svr.rc.VhostHttpsMuxer, err = vhost.NewHttpsMuxer(frpNet.WrapLogListener(l), 30*time.Second)
		if err != nil {
			err = fmt.Errorf("Create vhost httpsMuxer error, %v", err)
			return
		}
		log.Info("https service listen on %s:%d", cfg.ProxyBindAddr, cfg.VhostHttpsPort)
	}

	// frp tls listener
	tlsListener := svr.muxer.Listen(1, 1, func(data []byte) bool {
		return int(data[0]) == frpNet.FRP_TLS_HEAD_BYTE
	})
	svr.tlsListener = frpNet.WrapLogListener(tlsListener)

	// Create nat hole controller.
	if cfg.BindUdpPort > 0 {
		var nc *nathole.NatHoleController
		addr := fmt.Sprintf("%s:%d", cfg.BindAddr, cfg.BindUdpPort)
		nc, err = nathole.NewNatHoleController(addr)
		if err != nil {
			err = fmt.Errorf("Create nat hole controller error, %v", err)
			return
		}
		svr.rc.NatHoleController = nc
		log.Info("nat hole udp service listen on %s:%d", cfg.BindAddr, cfg.BindUdpPort)
	}

	var statsEnable bool
	// Create dashboard web server.
	if cfg.DashboardPort > 0 {
		err = svr.RunDashboardServer(cfg.DashboardAddr, cfg.DashboardPort)
		if err != nil {
			err = fmt.Errorf("Create dashboard web server error, %v", err)
			return
		}
		log.Info("Dashboard listen on %s:%d", cfg.DashboardAddr, cfg.DashboardPort)
		statsEnable = true
	}

	svr.statsCollector = stats.NewInternalCollector(statsEnable)
	return
}







func (svr *Service) Run() {
	if svr.rc.NatHoleController != nil {
		go svr.rc.NatHoleController.Run()
	}
	if g.GlbServerCfg.KcpBindPort > 0 {
		go svr.HandleListener(svr.kcpListener)
	}

	go svr.HandleListener(svr.websocketListener)
	go svr.HandleListener(svr.tlsListener)

	// tcp
	svr.HandleListener(svr.listener)
}



// 相对于客户端程序，服务端程序的功能就比较的单一，就是实现一个连接的转发。
// 程序的入口位于 /cmd/frps/main.go ，开始的步骤和客户端一样，都是解析配置文件，
// 最后调用了 /frp/server/server.go#Run(),这里面就调用一个 HandleListener(...) 函数，
// 里面是实现也比较的简单，一个标准的tcp阻塞函数，当有新的连接来的时候开启一个 goroutine ，
// 在这个 goroutine 中获取消息的内容，再根据不同的消息类型进行不同的处理，这里有3种类型，
// 分别为 Login，NewWorkConn 和 NewVisitorConn，很明显 Login 就是对客户端的授权处理，
// 而 NewWorkConn 就是客户端发起的服务。

func (svr *Service) HandleListener(l frpNet.Listener) {

	// Listen for incoming connections from client.
	for {

		// 接受 client 的连接请求
		c, err := l.Accept()
		if err != nil {
			log.Warn("Listener for incoming connections from client closed")
			return
		}
		log.Trace("start check TLS connection...")

		// 保存原始连接
		originConn := c

		// 封装 TLS
		c, err = frpNet.CheckAndEnableTLSServerConnWithTimeout(c, svr.tlsConfig, connReadTimeout)
		if err != nil {
			log.Warn("CheckAndEnableTLSServerConnWithTimeout error: %v", err)
			originConn.Close()
			continue
		}
		log.Trace("success check TLS connection")

		// Start a new goroutine for dealing connections.
		// 每来一个新的连接，就起一个 goroutine 去处理
		go func(frpConn frpNet.Conn) {

			// 请求处理函数
			dealFn := func(conn frpNet.Conn) {

				// 定义消息对象 rawMsg ，用来保存 frpc 发来的消息
				var rawMsg msg.Message

				// 设置读取消息的超时
				_ = conn.SetReadDeadline(time.Now().Add(connReadTimeout))

				// 将 frpc 发来的消息存到 rawMsg 中
				if rawMsg, err = msg.ReadMsg(conn); err != nil {
					log.Trace("Failed to read message: %v", err)
					conn.Close()
					return
				}

				// 取消读取超时设置，也即不设超时
				_ = conn.SetReadDeadline(time.Time{})

				//（重要）判断 client 发来的消息类型，进行相应处理
				switch m := rawMsg.(type) {

				// client 注册到 server
				case *msg.Login:
					// 为 client 新建 ctrl 并注册到 svr.ctlManager，用于维护 client 和 server 间的网络连接
					err = svr.RegisterControl(conn, m)
					// If login failed, send error message there.
					// Otherwise send success message in control's work goroutine.
					if err != nil {
						conn.Warn("%v", err)
						_ = msg.WriteMsg(conn, &msg.LoginResp{
							Version: version.Full(),
							Error:   err.Error(),
						})
						conn.Close()
					}

				// 收到 client 发来的 NewWorkConn 消息时，执行 svr.RegisterWorkConn() 把这个 conn 加入到 pool 中，供后续使用。
				case *msg.NewWorkConn:
					svr.RegisterWorkConn(conn, m)

				// NewVisitorConn 是用于 stcp, 也就是端对端加密通信的
				case *msg.NewVisitorConn:

					if err = svr.RegisterVisitorConn(conn, m); err != nil {

						conn.Warn("%v", err)
						_ = msg.WriteMsg(conn, &msg.NewVisitorConnResp{
							ProxyName: m.ProxyName,
							Error:     err.Error(),
						})
						conn.Close()

					} else {

						_ = msg.WriteMsg(conn, &msg.NewVisitorConnResp{
							ProxyName: m.ProxyName,
							Error:     "",
						})

					}

				default:
					log.Warn("Error message type for the new connection [%s]", conn.RemoteAddr().String())
					_ = conn.Close()
				}
			}


			// TCP 多路复用？
			if g.GlbServerCfg.TcpMux {

				fmuxCfg := fmux.DefaultConfig()
				fmuxCfg.KeepAliveInterval = 20 * time.Second
				fmuxCfg.LogOutput = ioutil.Discard
				session, err := fmux.Server(frpConn, fmuxCfg)
				if err != nil {
					log.Warn("Failed to create mux connection: %v", err)
					frpConn.Close()
					return
				}

				for {
					stream, err := session.AcceptStream()
					if err != nil {
						log.Debug("Accept new mux stream error: %v", err)
						session.Close()
						return
					}
					wrapConn := frpNet.WrapConn(stream)
					go dealFn(wrapConn)
				}
			} else {
				//
				dealFn(frpConn)
			}
		}(c)
	}
}


// client 注册 ctrl 到 svr.ctlManager，每个 client 有唯一的一个 ctrl 连接到 server
//
// 1. 检测 client 的版本号和 server 是否兼容
// 2. 获取 client 的 regist 消息里的加密 token 是否正确
// 3. 获取 client 的 RunID，若不存在则创建
// 4. 新建 ctrl 结构体，添加到 svr.ctlManager，并启动 ctl.Start()
func (svr *Service) RegisterControl(ctlConn frpNet.Conn, loginMsg *msg.Login) (err error) {
	ctlConn.Info("client login info: ip [%s] version [%s] hostname [%s] os [%s] arch [%s]",
				ctlConn.RemoteAddr().String(),
				loginMsg.Version,
				loginMsg.Hostname,
				loginMsg.Os,
				loginMsg.Arch,
				)

	// Check client version.
	// 1. 版本兼容性检查
	if ok, msg := version.Compat(loginMsg.Version); !ok {
		err = fmt.Errorf("%s", msg)
		return
	}

	// Check auth.
	// 2. 获取认证 authKey = hex.encode(md5(token + string(timestamp))), 比对 token 加密后字符串是否匹配。
	if util.GetAuthKey(g.GlbServerCfg.Token, loginMsg.Timestamp) != loginMsg.PrivilegeKey {
		err = fmt.Errorf("authorization failed")
		return
	}

	// If client's RunId is empty, it's a new client, we just create a new controller.
	// Otherwise, we check if there is one controller has the same run id.
	// If so, we release previous controller and start new one.

	// 3. 如果 RunID 为空，就新建一个
	if loginMsg.RunId == "" {
		loginMsg.RunId, err = util.RandId() // 生成随机的 8 位字符串 ID
		if err != nil {
			return
		}
	}

	// 4. 新建一个 control，记录 connection + loginMsg
	ctl := NewControl(svr.rc, svr.pxyManager, svr.statsCollector, ctlConn, loginMsg)

	// 5. 加入 ctl 管理
	if oldCtl := svr.ctlManager.Add(loginMsg.RunId, ctl); oldCtl != nil {
		oldCtl.allShutdown.WaitDone()
	}

	ctlConn.AddLogPrefix(loginMsg.RunId)

	// 6. 启动 ctl
	ctl.Start()

	// for statistics
	svr.statsCollector.Mark(stats.TypeNewClient, &stats.NewClientPayload{})

	go func() {
		// block until control closed
		ctl.WaitClosed()
		svr.ctlManager.Del(loginMsg.RunId, ctl)
	}()


	return
}

// RegisterWorkConn register a new work connection to control and proxies need it.
func (svr *Service) RegisterWorkConn(workConn frpNet.Conn, newMsg *msg.NewWorkConn) {

	// RunId 和 client 是一一对应的，所以这里是拿到 client 对应的 ctrl 结构体
	ctl, exist := svr.ctlManager.GetById(newMsg.RunId)
	if !exist {
		workConn.Warn("No client control found for run id [%s]", newMsg.RunId)
		return
	}

	// 把 workConn 放到 workConnCh 中，如果已经放满，就 close 掉。
	ctl.RegisterWorkConn(workConn)
	return
}

func (svr *Service) RegisterVisitorConn(visitorConn frpNet.Conn, newMsg *msg.NewVisitorConn) error {
	return svr.rc.VisitorManager.NewConn(
			newMsg.ProxyName,
			visitorConn,
			newMsg.Timestamp,
			newMsg.SignKey,
			newMsg.UseEncryption,
			newMsg.UseCompression,
		)
}

// Setup a bare-bones TLS config for the server
func generateTLSConfig() *tls.Config {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		panic(err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		panic(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{tlsCert}}
}
