package kademlia

import (
	"container/list"
	"fmt"
	"math/big"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"
	//"os"
	//"github.com/sirupsen/logrus"
)

const (
	k                    = 20
	alpha                = 3
	dialTimeOut          = 10 * time.Second
	callTimeOut          = 10 * time.Second
	findNodeTimeOut      = 250 * time.Millisecond
	nodeLookupTimeOut    = 5 * time.Second
	republishTimeOut     = 15 * time.Second
	republishDataTimeOut = 25 * time.Millisecond
	republishPeriod      = 15 * time.Second
	abandonPeriod        = 60 * time.Second
	refreshPeriod        = 3 * time.Second
)

var rnd *rand.Rand

func init() {
	//f, _ := os.Create("dht-test.log")
	//logrus.SetOutput(f)
	rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
}

type GetPairRPCReturnInfo struct {
	Ok    bool
	Value string
}

type NodeInfo struct {
	Addr string
	Id   *big.Int
}

type Node struct {
	Addr       NodeInfo
	online     bool
	buckets    [hashSize]list.List
	bucketLock sync.RWMutex
	data       Data
	dataLock   sync.RWMutex
	listener   net.Listener
	server     *rpc.Server
	start      chan bool
	quit       chan bool
}

func (node *Node) Init(addr string) {
	node.Addr = NodeInfo{addr, getHash(addr)}
	node.online = false
	node.dataLock.Lock()
	node.data.Init()
	node.start = make(chan bool, 1)
	node.quit = make(chan bool, 1)
}

//
// private methods
//

func (node *Node) remoteCall(addr string, method string, args interface{}, reply interface{}) error {
	// printLog := true
	// if printLog {
	// 	logrus.Infof("[%s] RemoteCall %s %s %v", node.Addr.Addr, addr, method, args)
	// }
	conn, err := net.DialTimeout("tcp", addr, dialTimeOut)
	if err != nil {
		// if printLog {
		// 	logrus.Infof("[%s] RemoteCall %s %s %v dial fails", node.Addr.Addr, addr, method, args)
		// }
		return err
	}
	// if printLog {
	// 	logrus.Infof("[%s] RemoteCall %s %s %v dial succeeds", node.Addr.Addr, addr, method, args)
	// }
	client := rpc.NewClient(conn)
	defer client.Close()
	done := make(chan error, 1)
	go func() {
		done <- client.Call(method, args, reply)
	}()
	select {
	case <-time.After(callTimeOut):
		// if printLog {
		// 	logrus.Errorf("[%s] RemoteCall %s %s %v call time out", node.Addr.Addr, addr, method, args)
		// }
		return fmt.Errorf("[%s] RemoteCall %s %s %v call time out", node.Addr.Addr, addr, method, args)
	case err = <-done:
		if err != nil {
			// if printLog {
			// 	logrus.Infof("[%s] RemoteCall %s %s %v remote call error", node.Addr.Addr, addr, method, args)
			// }
			return err
		}
	}
	// if printLog {
	// 	logrus.Infof("[%s] RemoteCall %s %s %v succeeds, reply = %v", node.Addr.Addr, addr, method, args, reply)
	// }
	return nil
}

func (node *Node) flush(target string, online bool) {
	id := getBucketId(node.Addr.Id, getHash(target))
	if id == -1 {
		return
	}
	node.bucketLock.Lock()
	defer node.bucketLock.Unlock()
	elem := listFind(&node.buckets[id], target, func(lhs string, rhs string) bool { return lhs == rhs })
	if !online {
		if elem != nil {
			node.buckets[id].Remove(elem)
		}
		return
	}
	if elem != nil {
		node.buckets[id].MoveToBack(elem)
		return
	}
	sz := node.buckets[id].Len()
	if sz < k {
		node.buckets[id].PushBack(target)
		return
	}
	frontValue := node.buckets[id].Front().Value
	if !node.Ping(frontValue.(string)) {
		node.buckets[id].Front().Value = target
	}
	node.buckets[id].MoveToBack(node.buckets[id].Front())
}

func (node *Node) findNode(callList []string, id *big.Int) []string {
	res := []string{}
	var lock sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(callList))
	for _, ip := range callList {
		go func(ip string) {
			defer wg.Done()
			tmp := []string{}
			err := node.remoteCall(ip, "Node.FindNodeRPC", id, &tmp)
			node.flush(ip, err == nil)
			if err != nil {
				//logrus.Errorf("[%s] FindNodeRPC error in findNode: %v", node.Addr.Addr, err)
				return
			}
			node.remoteCall(ip, "Node.NotifyRPC", node.Addr.Addr, nil)
			lock.Lock()
			res = append(res, tmp...)
			lock.Unlock()
		}(ip)
	}
	wg.Wait()
	return res
}

func (node *Node) nodeLookup(id *big.Int) []string {
	//logrus.Infof("[%s] lookup %v starts", node.Addr.Addr, id)
	var ls list.List
	done := make(chan bool, 1)
	cmp := func(lhs string, rhs string) bool {
		return new(big.Int).Xor(getHash(lhs), id).Cmp(new(big.Int).Xor(getHash(rhs), id)) == -1
	}
	go func() {
		tmp := []string{}
		node.FindNodeRPC(id, &tmp)
		flushList(&ls, tmp, cmp)
		//logrus.Infof("[%s] lookup %v in nodeLookup: ls = %v", node.Addr.Addr, id, listFilter(&ls, func(_ string) bool { return true }))
		call := make(map[string]struct{})
		notCalled := func(str string) bool {
			_, ok := call[str]
			return !ok
		}
		for {
			callList := listFilterSize(&ls, alpha, notCalled)
			foundNodeList := node.findNode(callList, id)
			flag := flushList(&ls, foundNodeList, cmp)
			for _, ip := range callList {
				call[ip] = struct{}{}
			}
			//logrus.Infof("[%s] lookup %v in nodeLookup: ls = %v", node.Addr.Addr, id, listFilter(&ls, func(_ string) bool { return true }))
			if flag {
				callList = listFilterSize(&ls, alpha, notCalled)
				foundNodeList = node.findNode(callList, id)
				flag = flushList(&ls, foundNodeList, cmp)
				for _, ip := range callList {
					call[ip] = struct{}{}
				}
				//logrus.Infof("[%s] lookup %v in nodeLookup: ls = %v", node.Addr.Addr, id, listFilter(&ls, func(_ string) bool { return true }))
			}
			if flag {
				break
			}
		}
		done <- true
	}()
	select {
	case <-done:
	case <-time.After(nodeLookupTimeOut):
		//logrus.Errorf("[%s] nodeLookup time out in nodeLookup", node.Addr.Addr)
	}
	return listFilterSize(&ls, k, func(_ string) bool { return true })
}

func (node *Node) findValue(callList []string, key string) (bool, []string, string) {
	//logrus.Infof("[%s] findValue: callList = %v, key = %s", node.Addr.Addr, callList, key)
	res := []string{}
	for _, ip := range callList {
		tmp := []string{}
		err := node.remoteCall(ip, "Node.FindNodeRPC", getHash(key), &tmp)
		node.flush(ip, err == nil)
		if err != nil {
			//logrus.Errorf("[%s] FindNodeRPC error in findValue: %v", node.Addr.Addr, err)
			continue
		}
		node.remoteCall(ip, "Node.NotifyRPC", node.Addr.Addr, nil)
		var getPairReturnInfo GetPairRPCReturnInfo
		err = node.remoteCall(ip, "Node.GetPairRPC", key, &getPairReturnInfo)
		if err != nil {
			//logrus.Errorf("[%s] GetPairRPC error in findValue: %v", node.Addr.Addr, err)
			continue
		}
		if getPairReturnInfo.Ok {
			return true, []string{}, getPairReturnInfo.Value
		}
		res = append(res, tmp...)
	}
	return false, res, ""
}

func (node *Node) republishData(kvPair KVPair, donePut chan bool) {
	nodeList := node.nodeLookup(getHash(kvPair.Key))
	//logrus.Infof("[%s] republish Data %v in %v", node.Addr.Addr, kvPair, nodeList)
	var wg sync.WaitGroup
	wg.Add(len(nodeList))
	for _, ip := range nodeList {
		go func(ip string) {
			defer wg.Done()
			if ip == node.Addr.Addr {
				node.data.putPair(kvPair.Key, kvPair.Value)
				//logrus.Infof("[%s] put Pair %v in republishData", node.Addr.Addr, kvPair)
			} else {
				err := node.remoteCall(ip, "Node.PutPairRPC", kvPair, nil)
				if err != nil {
					//logrus.Errorf("[%s] PutPairRPC error in republishData: %v", node.Addr.Addr, err)
				} else {
					//logrus.Infof("[%s] put Pair %v in republishData", ip, kvPair)
					node.remoteCall(ip, "Node.NotifyRPC", node.Addr.Addr, nil)
				}
				node.flush(ip, err == nil)
			}
		}(ip)
	}
	wg.Wait()
	donePut <- true
}

func (node *Node) republish(cond func(DataInfo) bool) {
	done := make(chan bool, 1)
	republishList := node.data.filter(cond)
	go func() {
		var wg sync.WaitGroup
		wg.Add(len(republishList))
		for _, kvPair := range republishList {
			go func(kvPair KVPair) {
				defer wg.Done()
				donePut := make(chan bool, 1)
				node.republishData(kvPair, donePut)
				select {
				case <-donePut:
				case <-time.After(republishDataTimeOut):
					//logrus.Errorf("[%s] republishData time out in republish: %v", node.Addr.Addr, kvPair)
				}
			}(kvPair)
		}
		wg.Wait()
		done <- true
	}()
	select {
	case <-done:
	case <-time.After(republishTimeOut):
		//logrus.Errorf("[%s] republish time out in republish", node.Addr.Addr)
	}
}

func (node *Node) abandon() {
	node.data.abandon()
}

func (node *Node) refresh() {
	refreshId := rnd.Intn(hashSize)
	node.bucketLock.RLock()
	sz := node.buckets[refreshId].Len()
	node.bucketLock.RUnlock()
	if sz <= 1 {
		//logrus.Infof("[%s] refresh bucket %d", node.Addr.Addr, refreshId)
		node.nodeLookup(exp[refreshId])
	}
}

func (node *Node) maintain() {
	go func() {
		for node.online {
			//logrus.Infof("[%s] starts republishing", node.Addr.Addr)
			node.republish(func(dataInfo DataInfo) bool { return time.Now().After(dataInfo.republishTime) })
			//logrus.Infof("[%s] stops republishing", node.Addr.Addr)
			time.Sleep(republishPeriod)
		}
	}()
	go func() {
		for node.online {
			//logrus.Infof("[%s] starts abandoning", node.Addr.Addr)
			node.abandon()
			//logrus.Infof("[%s] stops abandoning", node.Addr.Addr)
			time.Sleep(abandonPeriod)
		}
	}()
	go func() {
		for node.online {
			//logrus.Infof("[%s] starts refreshing", node.Addr.Addr)
			node.refresh()
			//logrus.Infof("[%s] ends refreshing", node.Addr.Addr)
			time.Sleep(refreshPeriod)
		}
	}()
}

//
// RPC Methods
//

func (node *Node) PingRPC(_ struct{}, _ *struct{}) error {
	if !node.online {
		return fmt.Errorf("[%s] Ping error: [%s] offline", node.Addr.Addr, node.Addr.Addr)
	}
	return nil
}

func (node *Node) GetPairRPC(key string, reply *GetPairRPCReturnInfo) error {
	ok, val := node.data.getPair(key)
	*reply = GetPairRPCReturnInfo{ok, val}
	return nil
}

func (node *Node) PutPairRPC(pair KVPair, _ *struct{}) error {
	node.data.putPair(pair.Key, pair.Value)
	return nil
}

func (node *Node) FindNodeRPC(id *big.Int, reply *[]string) error {
	done := make(chan bool, 1)
	go func() {
		i := getBucketId(node.Addr.Id, id)
		if i == -1 {
			*reply = append(*reply, node.Addr.Addr)
		} else {
			node.bucketLock.RLock()
			tmp := listFilter(&node.buckets[i], func(_ string) bool { return true })
			node.bucketLock.RUnlock()
			*reply = append(*reply, tmp...)
		}
		if len(*reply) == k {
			done <- true
			return
		}
		for j := i - 1; j >= 0; j-- {
			node.bucketLock.RLock()
			tmp := listFilter(&node.buckets[j], func(_ string) bool { return true })
			node.bucketLock.RUnlock()
			for _, val := range tmp {
				*reply = append(*reply, val)
				if len(*reply) == k {
					done <- true
					return
				}
			}
		}
		for j := i + 1; j < hashSize; j++ {
			node.bucketLock.RLock()
			tmp := listFilter(&node.buckets[j], func(_ string) bool { return true })
			node.bucketLock.RUnlock()
			for _, val := range tmp {
				*reply = append(*reply, val)
				if len(*reply) == k {
					done <- true
					return
				}
			}
		}
		if i != -1 {
			*reply = append(*reply, node.Addr.Addr)
		}
		done <- true
	}()
	select {
	case <-done:
		return nil
	case <-time.After(findNodeTimeOut):
		//logrus.Errorf("[%s] find node time out in FindNodeRPC", node.Addr.Addr)
		return fmt.Errorf("[%s] find node time out in FindNodeRPC", node.Addr.Addr)
	}
}

func (node *Node) NotifyRPC(addr string, reply *struct{}) error {
	node.flush(addr, true)
	return nil
}

//
// DHT methods
//

func (node *Node) Run() {
	node.server = rpc.NewServer()
	err := node.server.Register(node)
	if err != nil {
		//logrus.Errorf("[%s] Registor error in Run: %v", node.Addr.Addr, err)
		return
	}
	node.listener, err = net.Listen("tcp", node.Addr.Addr)
	if err != nil {
		//logrus.Errorf("[%s] TCP listener error in Run: %v", node.Addr.Addr, err)
		return
	}
	node.start <- true
	//logrus.Infof("[%s] starts running", node.Addr.Addr)
	go func() {
		for node.online {
			select {
			case <-node.quit:
				//logrus.Infof("[%s] stops running", node.Addr.Addr)
				return
			default:
				conn, err := node.listener.Accept()
				if err != nil {
					//logrus.Errorf("[%s] accept error: %v", node.Addr.Addr, err)
					//logrus.Infof("[%s] stops running", node.Addr.Addr)
					return
				}
				go node.server.ServeConn(conn)
			}
		}
	}()
	node.online = true
}

func (node *Node) Create() {
	<-node.start
	node.maintain()
}

func (node *Node) Join(addr string) bool {
	fmt.Printf("[%s] starts joining\n", node.Addr.Addr)
	<-node.start
	node.bucketLock.Lock()
	node.buckets[getBucketId(node.Addr.Id, getHash(addr))].PushBack(addr)
	node.bucketLock.Unlock()
	node.nodeLookup(node.Addr.Id)
	node.maintain()
	return true
}

func (node *Node) Ping(addr string) bool {
	err := node.remoteCall(addr, "Node.PingRPC", struct{}{}, nil)
	return err == nil
}

func (node *Node) Put(key string, value string) bool {
	//logrus.Infof("[%s] starts putting (key = %s, val = %s)", node.Addr.Addr, key, value)
	flag := false
	nodeList := node.nodeLookup(getHash(key))
	var wg sync.WaitGroup
	wg.Add(len(nodeList))
	for _, ip := range nodeList {
		go func(ip string) {
			defer wg.Done()
			if node.Addr.Addr == ip {
				node.data.putPair(key, value)
				//logrus.Infof("[%s] put pair: key = %v, val = %v", node.Addr.Addr, key, value)
				flag = true
				return
			}
			err := node.remoteCall(ip, "Node.PutPairRPC", KVPair{key, value}, nil)
			if err != nil {
				//logrus.Errorf("[%s] PutPairRPC error in Put: %v", node.Addr.Addr, err)
			} else {
				//logrus.Infof("[%s] put pair: key = %v, val = %v", ip, key, value)
				node.remoteCall(ip, "Node.NotifyRPC", node.Addr.Addr, nil)
				flag = true
			}
			node.flush(ip, err == nil)
		}(ip)
	}
	wg.Wait()
	//logrus.Infof("[%s] (key = %v, val = %v) put in %v", node.Addr.Addr, key, value, nodeList)
	return flag
}

func (node *Node) Get(key string) (bool, string) {
	//logrus.Infof("[%s] starts getting (key = %s)", node.Addr.Addr, key)
	var ls list.List
	id := getHash(key)
	cmp := func(lhs string, rhs string) bool {
		return new(big.Int).Xor(getHash(lhs), id).Cmp(new(big.Int).Xor(getHash(rhs), id)) == -1
	}
	tmp := []string{}
	node.FindNodeRPC(id, &tmp)
	flushList(&ls, tmp, cmp)
	//logrus.Infof("[%s] get %v in Get: ls = %v", node.Addr.Addr, id, listFilter(&ls, func(_ string) bool { return true }))
	call := make(map[string]struct{})
	notCalled := func(str string) bool {
		_, ok := call[str]
		return !ok
	}
	for {
		callList := listFilterSize(&ls, alpha, notCalled)
		ok, foundNodeList, val := node.findValue(callList, key)
		if ok {
			return true, val
		}
		flag := flushList(&ls, foundNodeList, cmp)
		for _, ip := range callList {
			call[ip] = struct{}{}
		}
		//logrus.Infof("[%s] get %v in Get: ls = %v", node.Addr.Addr, id, listFilter(&ls, func(_ string) bool { return true }))
		if flag {
			callList := listFilterSize(&ls, alpha, notCalled)
			ok, foundNodeList, val := node.findValue(callList, key)
			if ok {
				return true, val
			}
			flag = flushList(&ls, foundNodeList, cmp)
			for _, ip := range callList {
				call[ip] = struct{}{}
			}
			//logrus.Infof("[%s] get %v in Get: ls = %v", node.Addr.Addr, id, listFilter(&ls, func(_ string) bool { return true }))
		}
		if flag {
			break
		}
	}
	return false, ""
}

func (node *Node) Delete(key string) bool {
	return true
}

func (node *Node) Quit() {
	if !node.online {
		return
	}
	fmt.Printf("[%s] starts quiting\n", node.Addr.Addr)
	//logrus.Infof("[%s] starts quiting\n", node.Addr.Addr)
	node.republish(func(_ DataInfo) bool { return true })
	node.online = false
	node.quit <- true
	node.listener.Close()
}

func (node *Node) ForceQuit() {
	if !node.online {
		return
	}
	fmt.Printf("[%s] starts force quiting\n", node.Addr.Addr)
	node.online = false
	node.quit <- true
	node.listener.Close()
}
