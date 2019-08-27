// Copyright 2018 fatedier, fatedier@gmail.com
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

package mux

import (
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/fatedier/golib/errors"
	gnet "github.com/fatedier/golib/net"
)

const (
	// DefaultTimeout is the default length of time to wait for bytes we need.
	DefaultTimeout = 10 * time.Second
)




// Mux 结构体则相对来说复杂很多，先来看一下他的字段定义:

// 第一个字段 ln 是一个 Listener 接口；
// 然后 defaultLn 是一个 listener 的指针；
// lns 则是由 listener 的指针组成的切片，根据注释 // sorted by priority，我们终于知道 listener 的 priority 字段是干啥的了；
// 接下来是 maxNeedBytesNum 字段，好奇怪，比起 listener 的 needBytesNum 多了个 “Max” ，
// 所以我们推测这个值取得是 lns 以及 defaultLn 字段中所有 listener 中 needBytesNum 值最大的；
// 最后的mu字段我们就不说了。


// 需要注意的是：
// 我们可能会发现 Mux 和 listener 存在相互引用，但在 Go 中我们倒也不用太担心，
// 因为 Go 采用 “标记-回收”  或者其变种的垃圾回收算法，感兴趣可以参考 Golang 垃圾回收剖析。



type Mux struct {
	
	ln net.Listener

	defaultLn *listener

	// sorted by priority
	lns             []*listener // Mux 中包含了一个由 listener 组成的有序 slice ，当有连接事件产生时就会遍历这个 slice 找出合适的 listener 并将事件传给他。
	maxNeedBytesNum uint32

	mu sync.RWMutex
}

// NewMux() 很简单，需要注意的是 ln 字段存储的一般不是 listener 这样的非常规 Listener ，
// 一般是 TCPListener 这样具体的绑定了套接字的监听器。
func NewMux(ln net.Listener) (mux *Mux) {
	mux = &Mux{
		ln:  ln,
		lns: make([]*listener, 0),
	}
	return
}



// Listen() 基本做了三步:
//
// 1. 生成一个 listener 结构体实例，并获取互斥锁
// 2. 根据情况更新 needBytesNum 字段
// 3. 将新生成的 listener 实例按照优先级放入 lns 字段对应的 slice 中

// priority
func (mux *Mux) Listen(priority int, needBytesNum uint32, fn MatchFunc) net.Listener {

	// 1. build
	ln := &listener{
		c:            make(chan net.Conn),
		mux:          mux,
		priority:     priority,
		needBytesNum: needBytesNum,
		matchFn:      fn,
	}

	// lock
	mux.mu.Lock()
	defer mux.mu.Unlock()

	// 2. update mux.maxNeedBytesNum
	if needBytesNum > mux.maxNeedBytesNum {
		mux.maxNeedBytesNum = needBytesNum
	}

	// 3. append
	newlns := append(mux.copyLns(), ln)

	// 4. sort
	sort.Slice(newlns, func(i, j int) bool {
		if newlns[i].priority == newlns[j].priority {
			return newlns[i].needBytesNum < newlns[j].needBytesNum
		}
		return newlns[i].priority < newlns[j].priority
	})

	// 5. replace
	mux.lns = newlns
	return ln
}


func (mux *Mux) ListenHttp(priority int) net.Listener {
	return mux.Listen(priority, HttpNeedBytesNum, HttpMatchFunc)
}

func (mux *Mux) ListenHttps(priority int) net.Listener {
	return mux.Listen(priority, HttpsNeedBytesNum, HttpsMatchFunc)
}


// DefaultListener() 方法很简单，基本就是有则返回没有则生成然后返回的套路。
// 不过我们要注意 defaultLn 字段中的 listener 是不放入 lns 字段中的。
func (mux *Mux) DefaultListener() net.Listener {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	if mux.defaultLn == nil {
		mux.defaultLn = &listener{
			c:   make(chan net.Conn),
			mux: mux,
		}
	}
	return mux.defaultLn
}



// release方法意思很明确：
// 把对应的 listener 从 lns 中移除，并把结果返回，整个过程有互斥锁，我们回到存疑1，尽管有互斥锁，但在这种情况下：
// 当某个 goroutine 运行到 handleConn 已经执行到了第三阶段的开始状态(也就是还没有找到匹配的 listener )时，
// 且 Go 运行在多核状态下，当另一个 goroutine 运行完 listener 的 Close 方法时，这时就可能发生往一个已经关闭的 channel 中 send 数据。
func (mux *Mux) release(ln *listener) bool {
	result := false

	// 因为要修改 mux.lns，所以要加锁
	mux.mu.Lock()
	defer mux.mu.Unlock()

	// 获取拷贝，基于这个拷贝做修改
	lns := mux.copyLns()
	for i, l := range lns {
		// 如果 ln 存在于 lns 中，就置 result 为 true，然后把它从 lns 里删除
		if l == ln {
			lns = append(lns[:i], lns[i+1:]...)
			result = true
			break
		}
	}

	// 替换
	mux.lns = lns
	return result
}


// copyLns() 方法很简单，就是跟名字的含义一样，生成一个 lns 字段的副本并返回。
func (mux *Mux) copyLns() []*listener {
	lns := make([]*listener, 0, len(mux.lns))
	for _, l := range mux.lns {
		lns = append(lns, l)
	}
	return lns
}


// 一般来说，当我们调用 NewMux 函数以后，接下来就会调用 Server 方法，该方法基本上就是阻塞监听某个套接字，
// 当有连接建立成功后立即另起一个 goroutine 调用 handleConn 方法；
// 当连接建立失败根据 err 是否含有 Temporary 方法，如果有则执行并忽略错误，没有则返回错误。

// Serve handles connections from ln and multiplexes then across registered listeners.
func (mux *Mux) Serve() error {
	for {

		// Wait for the next connection.
		// If it returns a temporary error then simply retry.
		// If it returns any other error then exit immediately.


		// 1. 获取真正的网络连接 net.Conn
		conn, err := mux.ln.Accept()

		// 2. 若出错，检测是否是临时错误，是则重试，否则返回 err
		if err, ok := err.(interface {
			Temporary() bool
		}); ok && err.Temporary() {
			continue
		}

		if err != nil {
			return err
		}

		// 3. 检测 conn 的数据类型，交由对应类型的 listener 来处理，找不到类型则使用默认协议来处理。
		go mux.handleConn(conn)
	}
}


//handleConn() 方法也不算复杂，大体可以分为三步：
//
// 1. 获取当前 mux 的核心变量
// 2. 从 conn 中读取数据，注意：shareConn 和 rd 存在单向关系，如果从 rd 中读取数据的话，数据也会复制一份放到 shareConn 中，反过来就不成立了。
// 3. 读取到的数据会被遍历，最终选出与 matchFunc 匹配的最高优先级的 listener ，并将 shareConn 放入该 listener 的 c 字段中，
//    如果没有匹配到则放到 defaultLn 中的 c 字段中，如果 defaultLn 是 nil 的话就不处理，直接关闭 conn 。
func (mux *Mux) handleConn(conn net.Conn) {

	// 1. 获取当前 mux 的核心变量
	mux.mu.RLock()
	maxNeedBytesNum := mux.maxNeedBytesNum
	lns := mux.lns
	defaultLn := mux.defaultLn
	mux.mu.RUnlock()

	// 2. 构造共享的读取连接，从 rd 中读取数据的话，数据也会复制一份放到 shareConn 中
	sharedConn, rd := gnet.NewSharedConnSize(conn, int(maxNeedBytesNum))

	// 3. 第一次读取，只需要读取能够标识协议类型的 type_header 大小，maxNeedBytesNum 大小在创建 Mux 对象时就是确定的
	data := make([]byte, maxNeedBytesNum)

	conn.SetReadDeadline(time.Now().Add(DefaultTimeout))

	// 4. 从 rd 中读取 type_header 数据，以便于后面判断协议类型
	_, err := io.ReadFull(rd, data)
	if err != nil {
		conn.Close()
		return
	}
	conn.SetReadDeadline(time.Time{})


	// 5. lns 已经按优先级排序了，这里按优先级从高到低进行协议匹配，尝试找到一个 listener
	for _, ln := range lns {

		// 协议匹配，则需要把当前连接放到这个 listener.c 中，留待后续处理
		if match := ln.matchFn(data); match {
			// 把共享连接 sharedConn 写到 listener.c 中，捕获 panic 并转成 error
			err = errors.PanicToError(func() {
				ln.c <- sharedConn
			})
			// 如果出错，则可能 ln.c 被 close 掉了，这里处理一下。
			if err != nil {
				conn.Close()
			}
			// match 则返回
			return
		}
	}

	// No match listeners
	//
	// 没有在 lns 中找到匹配的协议 listener，就把 sharedConn 放到 defaultLn 中的 c 字段中。
	if defaultLn != nil {
		err = errors.PanicToError(func() {
			defaultLn.c <- sharedConn
		})
		if err != nil {
			conn.Close()
		}
		return
	}

	// No listeners for this connection, close it.
	//
	// 如果 defaultLn 是 nil 的话就不处理，直接关闭 conn 。
	conn.Close()
	return
}






// 刚看到这个结构体我们可能很迷惑，不知道都是干啥的，而且网络编程中一般 listener 这种东西要绑定在一个套接字上，
// 但很明显listener没有，不过其唯一跟套接字相关的可能是其 c 字段，c 是一个由 net.Conn 接口组成的 chanel ；
// 然后 mu 字段就是读写锁了，这个很简单；然后 mux 字段则是上面提到的两个结构体中的另一个结构体 Mux 的指针；
// 接下来到了 priority 字段上，顾名思义，这个似乎跟优先级有关系，暂且存疑；needBytesNum 则更有些蒙了，
// 不过感觉其是跟读取 byte 的数量有关系，最后是 matchFn 。

// 好，初步认识了这个结构体的结构后，我们看看其方法。其实现的三个方法使 listener 实现了 net.Listener 接口:
//
//// A Listener is a generic network listener for stream-oriented protocols.
////
//// Multiple goroutines may invoke methods on a Listener simultaneously.
//type Listener interface {
//	// Accept waits for and returns the next connection to the listener.
//	Accept() (Conn, error)
//
//	// Close closes the listener.
//	// Any blocked Accept operations will be unblocked and return errors.
//	Close() error
//
//	// Addr returns the listener's network address.
//	Addr() Addr
//}

type listener struct {
	mux *Mux

	priority     int		// 优先级，被用于排序 Mux.lns[] 数组
	needBytesNum uint32     // 当前监听的协议类型的 type_header 字段大小
	matchFn      MatchFunc  // matchFn 函数用来判断 data 是否属于当前监听的协议类型。

	c  chan net.Conn        // 每当有新的符合要求的连接 net.Conn 到达时，Serve() -> handleConn(conn net.Conn) 会将 conn 放到这个 channel 中。
	mu sync.RWMutex
}




// Accept() 方法很简单，就是从 c 这个由 net.Conn 组成的 channel 中，获取 net.Conn 对象，
// 好这里我们就明白了，这个 listener 和普通的不一样，他很特别，普通的 listener 监听的是套接字，
// 而他监听的是 channel ，另外，肯定有某个地方在不停的往 c 这个 channel 中放 net.Conn 。


// Accept waits for and returns the next connection to the listener.
func (ln *listener) Accept() (net.Conn, error) {
	// 从 c 中获取 net.Conn 对象（阻塞式），并返回
	conn, ok := <-ln.c
	if !ok {
		return nil, fmt.Errorf("network connection closed")
	}
	return conn, nil
}

// 接下来是 Close 方法：
// 我们暂且先把这个 ln.mux.release(ln) 放到一边，因为还不知道这个东西干了啥，暂且只需关注 close(ln.c)，
// 我们知道这个函数是用来关闭 channel c 的，go 推荐由发送端调用，但这里似乎 listener 是一个消费端，
// 可以看一下如何优雅的关闭 Go Channel ，看来重点在于 ln.mux.release(ln) 这里，我们暂且存疑[1]，留待下面解决。

// Close removes this listener from the parent mux and closes the channel.
func (ln *listener) Close() error {
	//
	if ok := ln.mux.release(ln); ok {
		// Close done to signal to any RLock holders to release their lock.
		close(ln.c)
	}
	return nil
}


// 最后是 Addr() 方法：
// 在这里，mu 字段就用上了，加读锁，然后返回 mux 字段中的 ln 字段的 Addr 方法。
// 也就是这句 return ln.mux.ln.Addr() 。

func (ln *listener) Addr() net.Addr {
	//
	if ln.mux == nil {
		return nil
	}
	ln.mux.mu.RLock()
	defer ln.mux.mu.RUnlock()
	if ln.mux.ln == nil {
		return nil
	}
	return ln.mux.ln.Addr()
}
