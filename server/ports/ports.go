package ports

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)



// TCP/IP 协议中的服务端口，端口号的范围从 0 到 65535，其中全 0 没有意义，所以可用端口 1-65535:
// 1：公认端口: 从 0 到 1023
// 2：注册端口: 从 1024 到 49151
// 3: 动态或私有端口（私人端口号）: 一般从 49152 到 65535

// Linux 中有限定端口的使用范围，如果我要为程序预留某些端口，那么需要控制这个端口范围。
// 在 /proc/sys/net/ipv4/ip_local_port_range 定义了本地 TCP/UDP 的端口范围，
// 你可以在 /etc/sysctl.conf 里面定义n et.ipv4.ip_local_port_range = 1024 65000


const (
	MinPort                    = 1
	MaxPort                    = 65535
	MaxPortReservedDuration    = time.Duration(24) * time.Hour  // 24 * Hour
	CleanReservedPortsInterval = time.Hour 						// 1 * Hour
)

var (
	ErrPortAlreadyUsed = errors.New("port already used")
	ErrPortNotAllowed  = errors.New("port not allowed")
	ErrPortUnAvailable = errors.New("port unavailable")
	ErrNoAvailablePort = errors.New("no available port")
)



type PortCtx struct {
	ProxyName  string       // 代理名称
	Port       int          // 监听端口
	Closed     bool         // 是否关闭
	UpdateTime time.Time    // 更新时间
}

type PortManager struct {
	reservedPorts map[string]*PortCtx  // 保存 proxy name 和 portCtx 的映射关系
	usedPorts     map[int]*PortCtx     // 已使用端口号
	freePorts     map[int]struct{}     // 空闲端口号

	bindAddr string 	// 本协议类型的监听地址
	netType  string		// 网络协议类型
	mu       sync.Mutex // 锁
}


// netType: 协议类型， tcp or udp
// bindAddr: 本地监听 IP 地址
// allowPorts: 允许监听的端口
func NewPortManager(netType string, bindAddr string, allowPorts map[int]struct{}) *PortManager {

	pm := &PortManager{
		reservedPorts: make(map[string]*PortCtx),
		usedPorts:     make(map[int]*PortCtx),
		freePorts:     make(map[int]struct{}),
		bindAddr:      bindAddr,
		netType:       netType,
	}

	// 如果配置了代理端口，则用其初始化 pm.freePorts[]，否则允许代理任意端口
	if len(allowPorts) > 0 {
		for port, _ := range allowPorts {
			pm.freePorts[port] = struct{}{}
		}
	} else {
		for i := MinPort; i <= MaxPort; i++ {
			pm.freePorts[i] = struct{}{}
		}
	}

	//
	go pm.cleanReservedPortsWorker()
	return pm
}



// 为 name 代理申请可被监听的端口号 port，返回真正能用来监听的端口 realPort 和 错误信息。
//
// name: proxy name
// port: 监听端口
func (pm *PortManager) Acquire(name string, port int) (realPort int, err error) {

	portCtx := &PortCtx{
		ProxyName:  name,
		Closed:     false,
		UpdateTime: time.Now(),
	}

	var ok bool

	// 成对加解锁
	pm.mu.Lock()
	defer func() {
		if err == nil {
			portCtx.Port = realPort
		}
		pm.mu.Unlock()
	}()


	// 如果 port 为 0，则未指定监听端口，可以任意指定
	if port == 0 {
		// 检查 proxy name 是否已经存在
		if ctx, ok := pm.reservedPorts[name]; ok {
			// 若存在，则检查 port 能否被 listen
			if pm.isPortAvailable(ctx.Port) {
				// 如果当前 port 可被 listen，则使用该 port，同时更新 pm.usedPorts[]、pm.reservedPorts[]、pm.freePorts[] 三个 map 的关联数据。
				realPort = ctx.Port // 设置 realPort 用于返回
				pm.usedPorts[realPort] = portCtx
				pm.reservedPorts[name] = portCtx
				delete(pm.freePorts, realPort)
				return // 返回
			}
			// port 不能被 listen，可能已经被占用
		}
		// proxy name 的监听不存在
	}
	// port != 0



	// 如果未指定监听端口，可以随机获取
	if port == 0 {

		// 最多探查 5 次
		count := 0
		maxTryTimes := 5

		// 遍历 pm.freePorts 寻找第一个可用于 listen 的端口，【注意】这里潜在利用了 map 遍历的随机性
		for k, _ := range pm.freePorts {

			// 最多探查 maxTryTimes 次，避免阻塞太久
			count++
			if count > maxTryTimes {
				break
			}

			// 当前端口可被 listen，则使用该 port，同时更新 pm.usedPorts[]、pm.reservedPorts[]、pm.freePorts[] 三个 map 的关联数据。
			if pm.isPortAvailable(k) {
				realPort = k
				pm.usedPorts[realPort] = portCtx
				pm.reservedPorts[name] = portCtx
				delete(pm.freePorts, realPort)
				break
			}
		}

		// 如果连续 maxTryTimes 次都找不到可用端口，就报错
		if realPort == 0 {
			err = ErrNoAvailablePort
		}


	} else {

		// 如果明确指定了监听端口，则检查其是否可用，不可用则报错

		// 1. 是否空闲
		if _, ok = pm.freePorts[port]; ok {

			// 1.1 是否可用
			if pm.isPortAvailable(port) {

				// 1.1.1 可用则使用该 port，同时更新 pm.usedPorts[]、pm.reservedPorts[]、pm.freePorts[] 三个 map 的关联数据。
				realPort = port
				pm.usedPorts[realPort] = portCtx
				pm.reservedPorts[name] = portCtx
				delete(pm.freePorts, realPort)

			} else {
				// 1.1.2 不可用则报错
				err = ErrPortUnAvailable

			}

		// 2. 端口非空闲
		} else {
			// 2.1 使用中，报错
			if _, ok = pm.usedPorts[port]; ok {
				err = ErrPortAlreadyUsed

			// 2.2 不可用，报错
			} else {
				err = ErrPortNotAllowed
			}
		}
	}
	return
}


// 检查 port 能否被 listen
func (pm *PortManager) isPortAvailable(port int) bool {

	if pm.netType == "udp" {
		// 生成 udp 监听地址
		addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", pm.bindAddr, port))
		if err != nil {
			return false
		}
		// 执行 listenUDP
		l, err := net.ListenUDP("udp", addr)
		// listen 失败则返回 false
		if err != nil {
			return false
		}
		// listen 成功则返回 true
		l.Close()
		return true

	} else {
		l, err := net.Listen(pm.netType, fmt.Sprintf("%s:%d", pm.bindAddr, port))
		if err != nil {
			return false
		}
		l.Close()
		return true
	}
}


// 如果 port 位于 usedPorts[] 中，则把它移动到 freePorts[] 中，并同时把关联的 PortCtx.Close 置为 true，同时更新 ctx.UpdateTime。
func (pm *PortManager) Release(port int) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if ctx, ok := pm.usedPorts[port]; ok {
		pm.freePorts[port] = struct{}{}
		delete(pm.usedPorts, port)
		ctx.Closed = true
		ctx.UpdateTime = time.Now()
	}
}



// Release reserved port if it isn't used in last 24 hours.
//
func (pm *PortManager) cleanReservedPortsWorker() {
	for {
		// 每小时检查一次
		time.Sleep(CleanReservedPortsInterval)
		pm.mu.Lock()
		// 遍历所有 proxy，获取对应的 portCtx，如果对应端口已经被关闭(Release)，且最近的更新时间超过 24h，则从 pm.reservedPorts 删除这个 proxy 
		for name, ctx := range pm.reservedPorts {
			if ctx.Closed && time.Since(ctx.UpdateTime) > MaxPortReservedDuration {
				delete(pm.reservedPorts, name)
			}
		}
		pm.mu.Unlock()
	}
}
