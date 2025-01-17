// implementation of chord protocal
package chord

import (
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

var rnd *rand.Rand

func init() {
	//f, _ := os.Create("dht-test.log")
	//logrus.SetOutput(f)
	rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
}

type NodeInfo struct {
	Addr string
	Id   *big.Int
}

const succSize = 10
const pingWaitTime = 300 * time.Millisecond
const stabilizationPeriod = 50 * time.Millisecond
const fingerFixPeriod = 50 * time.Millisecond

type Node struct {
	Addr        NodeInfo
	online      bool
	finger      [hashSize + 1]NodeInfo
	fingerStart [hashSize + 1]*big.Int
	fingerLock  sync.RWMutex
	pred        NodeInfo
	predLock    sync.RWMutex
	succList    [succSize]NodeInfo
	succLock    sync.RWMutex
	data        map[string]string
	dataLock    sync.RWMutex
	backup      map[string]string
	backupLock  sync.RWMutex
	listener    net.Listener
	server      *rpc.Server
	start       chan bool
	quit        chan bool
	quitLock    sync.Mutex // block maintenance when quiting
}

// Pair is used to store a key-value pair.
// Note: It must be exported (i.e., Capitalized) so that it can be
// used as the argument type of RPC methods.
type Pair struct {
	Key   string
	Value string
}

// Initialize a node.
// Addr is the address and port number of the node, e.g., "localhost:1234".
func (node *Node) Init(addr string) {
	node.Addr = NodeInfo{addr, getHash(addr)}
	node.online = false
	node.dataLock.Lock()
	node.data = make(map[string]string)
	node.dataLock.Unlock()
	node.backupLock.Lock()
	node.backup = make(map[string]string)
	node.backupLock.Unlock()
	for i := 1; i <= hashSize; i++ {
		node.fingerStart[i] = add(node.Addr.Id, i-1)
	}
	node.start = make(chan bool, 1)
	node.quit = make(chan bool, 1)
}

//
// private methods
//

// Call the RPC method at addr.
// TODO: Re-connect to the client every time can be slow. You can use connection pool to improve the performance.
func (node *Node) remoteCall(addr string, method string, args interface{}, reply interface{}) error {
	// printLog := (method == "Node.SetBackupRPC" || method == "Node.UpdateBackupRPC" || method == "Node.GetBackupRPC" || method == "Node.AddDataRPC")
	// if printLog {
	// 	logrus.Infof("[%s] RemoteCall %s %s %v", node.Addr.Addr, addr, method, args)
	// }
	conn, err := net.DialTimeout("tcp", addr, pingWaitTime)
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
	case <-time.After(pingWaitTime):
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
func (node *Node) maintain() {
	go func() {
		for node.online {
			node.quitLock.Lock()
			err := node.stabilize()
			node.quitLock.Unlock()
			if err != nil {
				//logrus.Errorf("[%s] Stabilization fails: %v", node.Addr.Addr, err)
				continue
			}
			time.Sleep(stabilizationPeriod)
		}
		//logrus.Infof("[%s] Stabilization ends: (Id = %v)", node.Addr.Addr, node.Addr.Id)
	}()
	go func() {
		for node.online {
			err := node.fixFinger()
			if err != nil {
				//logrus.Errorf("[%s] Finger fix fails: %v", node.Addr.Addr, err)
				continue
			}
			time.Sleep(fingerFixPeriod)
		}
		//logrus.Infof("[%s] Finger fix ends (Id = %v)", node.Addr.Addr, node.Addr.Id)
	}()

}

// Fix successor list
func (node *Node) fixSuccList() error {
	if !node.online {
		return nil
	}
	//logrus.Infof("[%s] Successor list fix begins", node.Addr.Addr)
	node.succLock.RLock()
	succ := node.succList[0]
	node.succLock.RUnlock()
	//logrus.Infof("[%s] Ping successor in fixSuccList %s", node.Addr.Addr, succ.Addr)
	if node.Ping(succ.Addr) {
		var succSuccList [succSize]NodeInfo
		err := node.remoteCall(succ.Addr, "Node.GetSuccListRPC", struct{}{}, &succSuccList)
		//logrus.Infof("[%s] succSuccList = %v, err = %v", node.Addr.Addr, succSuccList, err)
		if err == nil {
			node.succLock.Lock()
			for i := 1; i < succSize; i++ {
				node.succList[i].Addr = succSuccList[i-1].Addr
				node.succList[i].Id = getHash(node.succList[i].Addr)
			}
			node.succLock.Unlock()
			//logrus.Infof("[%s] Successor list fix ends", node.Addr.Addr)
			return nil
		}
	}
	//logrus.Infof("[%s] node.succList[0] fails", node.Addr.Addr)
	node.succLock.RLock()
	succListCopy := node.succList
	node.succLock.RUnlock()
	for i := 1; i < succSize; i++ {
		if node.Ping(succListCopy[i].Addr) {
			var newSuccList [succSize]NodeInfo
			err := node.remoteCall(succListCopy[i].Addr, "Node.GetSuccListRPC", struct{}{}, &newSuccList)
			if err != nil {
				//logrus.Errorf("[%s] Get successor list error: %v", node.Addr.Addr, err)
				continue
			}
			node.succLock.Lock()
			node.succList[0].Addr = succListCopy[i].Addr
			node.succList[0].Id = getHash(node.succList[0].Addr)
			for j := 1; j < succSize; j++ {
				node.succList[j].Addr = newSuccList[j-1].Addr
				node.succList[j].Id = getHash(node.succList[j].Addr)
			}
			node.succLock.Unlock()
			backup := make(map[string]string)
			node.remoteCall(succListCopy[i].Addr, "Node.GetBackupRPC", struct{}{}, &backup)
			node.remoteCall(succListCopy[i].Addr, "Node.AddDataRPC", &backup, nil)
			node.UpdateBackupRPC(struct{}{}, nil)
			node.remoteCall(succListCopy[i].Addr, "Node.UpdateBackupRPC", struct{}{}, nil)
			if i != 1 {
				//logrus.Warningf("[%s] lose data in successor", node.Addr.Addr)
			}
			//logrus.Infof("[%s] Successor list fix ends", node.Addr.Addr)
			return nil
		}
	}
	return fmt.Errorf("[%s] Every node in successor list quits", node.Addr.Addr)
}

// Fix successor list and predecessor
func (node *Node) stabilize() error {
	if !node.online {
		return nil
	}
	//logrus.Infof("[%s] Stabilization begins", node.Addr.Addr)
	node.fixSuccList()
	var succ NodeInfo
	node.GetSuccRPC(struct{}{}, &succ)
	//logrus.Infof("[%s] Ping successor in stabilization %s", node.Addr.Addr, succ.Addr)
	if !node.Ping(succ.Addr) {
		//logrus.Errorf("[%s] Get successor fails in stabilization", node.Addr.Addr)
		return fmt.Errorf("[%s] Get successor fails in stabilization", node.Addr.Addr)
	}
	var x NodeInfo
	node.remoteCall(succ.Addr, "Node.GetPredRPC", struct{}{}, &x)
	//logrus.Infof("[%s] Get x %s in stabilization", node.Addr.Addr, x.Addr)
	if !node.Ping(x.Addr) {
		//logrus.Errorf("[%s] Get x fails", node.Addr.Addr)
	} else {
		//logrus.Errorf("[%s] Get x succeeds", node.Addr.Addr)
		//logrus.Infof("[%s] stabilize: beg = %v, end = %v, id = %v", node.Addr.Addr, node.Addr.Id, succ.Id, x.Id)
		if belong(false, false, node.Addr.Id, succ.Id, x.Id) {
			node.succLock.Lock()
			node.succList[0].Addr = x.Addr
			node.succList[0].Id = getHash(node.succList[0].Addr)
			node.succLock.Unlock()
		}
	}
	err := node.remoteCall(succ.Addr, "Node.NotifyRPC", &node.Addr, nil)
	if err != nil {
		//logrus.Errorf("[%s] Notify fails: %v", succ.Addr, err)
		return err
	}
	//logrus.Infof("[%s] Stabilization ends", node.Addr.Addr)
	return nil
}

// Fix finger
func (node *Node) fixFinger() error {
	if !node.online {
		return nil
	}
	rndInt := rnd.Intn(hashSize-1) + 2
	var nodeInfo NodeInfo
	err := node.FindSuccRPC(node.fingerStart[rndInt], &nodeInfo)
	if err != nil {
		//logrus.Errorf("[%s] Finger fix error: %v", node.Addr.Addr, err)
		return err
	}
	node.fingerLock.Lock()
	node.finger[rndInt].Addr = nodeInfo.Addr
	node.finger[rndInt].Id = getHash(node.finger[rndInt].Addr)
	node.fingerLock.Unlock()
	//logrus.Infof("[%s] Finger fix ends", node.Addr.Addr)
	return nil
}

//
// RPC Methods
//

// Get first node online in the successor list
func (node *Node) GetSuccRPC(_ struct{}, reply *NodeInfo) error {
	node.succLock.RLock()
	defer node.succLock.RUnlock()
	for _, nodeInfo := range node.succList {
		if node.Ping(nodeInfo.Addr) {
			(*reply).Addr = nodeInfo.Addr
			(*reply).Id = getHash((*reply).Addr)
			return nil
		}
	}
	return fmt.Errorf("[%s] Every node in successor list fails", node.Addr.Addr)
}

func (node *Node) GetSuccListRPC(_ struct{}, reply *[succSize]NodeInfo) error {
	node.succLock.RLock()
	*reply = node.succList
	node.succLock.RUnlock()
	return nil
}

func (node *Node) GetPredRPC(_ struct{}, reply *NodeInfo) error {
	node.predLock.RLock()
	(*reply).Addr = node.pred.Addr
	(*reply).Id = getHash((*reply).Addr)
	node.predLock.RUnlock()
	return nil
}

func (node *Node) SetSuccListRPC(succList [succSize]NodeInfo, reply *struct{}) error {
	node.succLock.Lock()
	node.succList = succList
	node.succLock.Unlock()
	return nil
}

func (node *Node) PingRPC(_ struct{}, _ *struct{}) error {
	if !node.online {
		return fmt.Errorf("Ping error: [%s] offline", node.Addr.Addr)
	}
	return nil
}

func (node *Node) FindSuccRPC(id *big.Int, reply *NodeInfo) error {
	//logrus.Infof("[%s] call FindPredRPC (id = %v) in FindSuccRPC", node.Addr.Addr, id)
	var predNode NodeInfo
	err := node.FindPredRPC(id, &predNode)
	if err != nil {
		return err
	}
	err = node.remoteCall(predNode.Addr, "Node.GetSuccRPC", struct{}{}, reply)
	if err != nil {
		return err
	}
	return nil
}

func (node *Node) FindPredRPC(id *big.Int, reply *NodeInfo) error {
	var succ NodeInfo
	err := node.GetSuccRPC(struct{}{}, &succ)
	if err != nil {
		//logrus.Errorf("[%s] get successor fails in FindPredRPC: %v", node.Addr.Addr, err)
	}
	//logrus.Infof("[%s] FindPredRPC: beg = %v, end = %v, id = %v", node.Addr.Addr, node.Addr.Id, succ.Id, id)
	if belong(false, true, node.Addr.Id, succ.Id, id) {
		*reply = node.Addr
		return nil
	}
	//logrus.Infof("[%s] Find closest preceding finger (id = %v) in FindPredRPC", node.Addr.Addr, id)
	var nodeTmp NodeInfo
	node.FindClosestPrecedingFingerRPC(id, &nodeTmp)
	//logrus.Infof("[%s] Find closest preceding finger (id = %v) in FindPredRPC, result = %v", node.Addr.Addr, id, nodeTmp)
	err = node.remoteCall(nodeTmp.Addr, "Node.FindPredRPC", id, reply)
	if err != nil {
		//logrus.Errorf("[%s] Find predecessor error: %v", node.Addr.Addr, err)
		return err
	}
	return nil
}

func (node *Node) FindClosestPrecedingFingerRPC(id *big.Int, reply *NodeInfo) error {
	node.fingerLock.RLock()
	defer node.fingerLock.RUnlock()
	for i := hashSize; i >= 1; i-- {
		var nodeInfo NodeInfo
		if i >= 2 {
			nodeInfo = node.finger[i]
		} else {
			err := node.GetSuccRPC(struct{}{}, &nodeInfo)
			if err != nil {
				continue
			}
		}
		//logrus.Infof("[%s] Find closest preceding finger, ping %s", node.Addr.Addr, nodeInfo.Addr)
		if node.Ping(nodeInfo.Addr) {
			//logrus.Infof("[%s] FindClosestPrecedingFingerRPC: beg = %v, end = %v, id = %v", node.Addr.Addr, node.Addr.Id, id, nodeInfo.Id)
		}
		if node.Ping(nodeInfo.Addr) && belong(false, false, node.Addr.Id, id, nodeInfo.Id) {
			(*reply).Addr = nodeInfo.Addr
			(*reply).Id = getHash((*reply).Addr)
			return nil
		}
	}
	*reply = node.Addr
	return nil
}

func (node *Node) NotifyRPC(n NodeInfo, _ *struct{}) error {
	node.predLock.RLock()
	//logrus.Infof("[%s] notify begins: new_pred = %s, pred = %s", node.Addr.Addr, n.Addr, node.pred.Addr)
	pred := node.pred
	node.predLock.RUnlock()
	if node.Ping(pred.Addr) {
		//logrus.Infof("[%s] NotifyRPC: beg = %v, end = %v, id = %v", node.Addr.Addr, pred.Id, node.Addr.Id, n.Id)
	}
	if !node.Ping(pred.Addr) || belong(false, false, pred.Id, node.Addr.Id, n.Id) {
		//logrus.Infof("[%s] notify before changing pred", node.Addr.Addr)
		node.predLock.Lock()
		node.pred.Addr = n.Addr
		node.pred.Id = getHash(node.pred.Addr)
		node.predLock.Unlock()
		//logrus.Infof("[%s] notify after changing pred", node.Addr.Addr)
	}
	return nil
}

func (node *Node) GetPairRPC(key string, val *string) error {
	node.dataLock.RLock()
	v, ok := node.data[key]
	node.dataLock.RUnlock()
	if ok {
		*val = v
		return nil
	}
	*val = ""
	return fmt.Errorf("[%s] Cannot find data (key = %v)", node.Addr.Addr, key)
}

func (node *Node) PutPairRPC(pair Pair, _ *struct{}) error {
	node.dataLock.Lock()
	node.data[pair.Key] = pair.Value
	node.dataLock.Unlock()
	return nil
}

func (node *Node) DeletePairRPC(key string, _ *struct{}) error {
	node.dataLock.Lock()
	defer node.dataLock.Unlock()
	_, ok := node.data[key]
	if ok {
		delete(node.data, key)
		return nil
	}
	return fmt.Errorf("[%s] Cannot find key in data: %v", node.Addr.Addr, key)
}

// Add the elements in data to node.data
func (node *Node) AddDataRPC(data *map[string]string, _ *struct{}) error {
	node.dataLock.Lock()
	for k, v := range *data {
		node.data[k] = v
	}
	node.dataLock.Unlock()
	return nil
}

// Move the elements in node.data that is less than or equal to id to data, and delete these elements from node.data.
func (node *Node) RemoveDataRPC(id *big.Int, data *map[string]string) error {
	node.predLock.RLock()
	pred := node.pred
	node.predLock.RUnlock()
	node.dataLock.Lock()
	for k, v := range node.data {
		//logrus.Infof("[%s] RemoveDataRPC: beg = %v, end = %v, id = %v", node.Addr.Addr, pred.Id, id, getHash(k))
		if belong(false, true, pred.Id, id, getHash(k)) {
			(*data)[k] = v
			delete(node.data, k)
		}
	}
	node.dataLock.Unlock()
	return nil
}

func (node *Node) GetBackupRPC(_ struct{}, backup *map[string]string) error {
	(*backup) = make(map[string]string)
	node.backupLock.RLock()
	for k, v := range node.backup {
		(*backup)[k] = v
	}
	node.backupLock.RUnlock()
	return nil
}

func (node *Node) SetBackupRPC(backup *map[string]string, _ *struct{}) error {
	node.backupLock.Lock()
	node.backup = make(map[string]string)
	for k, v := range *backup {
		node.backup[k] = v
	}
	node.backupLock.Unlock()
	return nil
}

func (node *Node) UpdateBackupRPC(_ struct{}, _ *struct{}) error {
	var succ NodeInfo
	err := node.GetSuccRPC(struct{}{}, &succ)
	if err != nil {
		//logrus.Errorf("[%s] get successor fails in UpdateBackupRPC: %v", node.Addr.Addr, err)
	}
	node.dataLock.RLock()
	err = node.remoteCall(succ.Addr, "Node.SetBackupRPC", &node.data, nil)
	node.dataLock.RUnlock()
	if err != nil {
		//logrus.Errorf("[%s] update backup fails: %v", node.Addr.Addr, err)
	}
	return err
}

func (node *Node) LockQuitLockRPC(_ struct{}, _ *struct{}) error {
	node.quitLock.Lock()
	return nil
}

func (node *Node) UnlockQuitLockRPC(_ struct{}, _ *struct{}) error {
	node.quitLock.Unlock()
	return nil
}

//
// DHT methods
//

func (node *Node) Run() {
	node.server = rpc.NewServer()
	err := node.server.Register(node)
	if err != nil {
		//logrus.Errorln("Registor error:", node.Addr.Addr, err)
		return
	}
	node.listener, err = net.Listen("tcp", node.Addr.Addr)
	if err != nil {
		//logrus.Errorln("TCP listener error:", node.Addr.Addr, err)
		return
	}
	close(node.start)
	//logrus.Infoln("Run:", node.Addr.Addr)
	go func() {
		for node.online {
			select {
			case <-node.quit:
				//logrus.Infof("Run end: [%s] (Id = %v)", node.Addr.Addr, node.Addr.Id)
				return
			default:
				conn, err := node.listener.Accept()
				if err != nil {
					//logrus.Errorln("Accept error:", err)
					return
				}
				go node.server.ServeConn(conn)
			}
		}
	}()
	node.online = true
}

// Create a new network
func (node *Node) Create() {
	node.fingerLock.Lock()
	for i := 1; i <= hashSize; i++ {
		node.finger[i].Addr = node.Addr.Addr
		node.finger[i].Id = getHash(node.finger[i].Addr)
	}
	node.fingerLock.Unlock()
	node.predLock.Lock()
	node.pred = node.Addr
	node.predLock.Unlock()
	node.succLock.Lock()
	for i := range node.succList {
		node.succList[i].Addr = node.Addr.Addr
		node.succList[i].Id = getHash(node.succList[i].Addr)
	}
	node.succLock.Unlock()
	<-node.start
	//logrus.Infof("Create: Node [%s] (Id = %v)", node.Addr.Addr, node.Addr.Id)
	node.maintain()
}

func (node *Node) Join(addr string) bool {
	//fmt.Printf("[%s] starts joining\n", node.Addr.Addr)
	node.predLock.Lock()
	node.pred = NodeInfo{}
	node.predLock.Unlock()
	<-node.start
	if !node.Ping(addr) {
		//logrus.Errorf("[%s] Join fails: [%s] shut down", node.Addr.Addr, addr)
		return false
	}
	var succ NodeInfo
	err := node.remoteCall(addr, "Node.FindSuccRPC", node.Addr.Id, &succ)
	if err != nil {
		//logrus.Errorf("[%s] Join fails: %v", node.Addr.Addr, err)
		return false
	}
	var succList [succSize]NodeInfo
	err = node.remoteCall(succ.Addr, "Node.GetSuccListRPC", struct{}{}, &succList)
	if err != nil {
		//logrus.Errorf("[%s] Join fails: %v", node.Addr.Addr, err)
		return false
	}
	node.succLock.Lock()
	node.succList[0].Addr = succ.Addr
	node.succList[0].Id = getHash(node.succList[0].Addr)
	for i := 1; i < succSize; i++ {
		node.succList[i].Addr = succList[i-1].Addr
		node.succList[i].Id = getHash(node.succList[i].Addr)
	}
	node.succLock.Unlock()
	node.dataLock.Lock()
	err = node.remoteCall(succ.Addr, "Node.RemoveDataRPC", node.Addr.Id, &node.data)
	if err != nil {
		//logrus.Errorf("[%s] Get data from successor fails in join: %v", node.Addr.Addr, err)
		return false
	}
	node.dataLock.Unlock()
	var succSucc NodeInfo
	node.remoteCall(succ.Addr, "Node.GetPredRPC", struct{}{}, &succSucc)
	backup := make(map[string]string)
	node.remoteCall(succ.Addr, "Node.GetBackupRPC", struct{}{}, &backup)
	node.backupLock.Lock()
	if succ.Addr == succSucc.Addr {
		for k, v := range backup {
			//logrus.Infof("[%s] Join: beg = %v, end = %v, id = %v", node.Addr.Addr, node.Addr.Id, succ.Id, getHash(k))
			if belong(false, true, node.Addr.Id, succ.Id, getHash(k)) {
				node.backup[k] = v
			}
		}
	} else {
		node.backup = backup
	}
	node.backupLock.Unlock()
	node.UpdateBackupRPC(struct{}{}, nil)
	err = node.remoteCall(succ.Addr, "Node.UpdateBackupRPC", struct{}{}, nil)
	if err != nil {
		//logrus.Errorf("[%s] node's successor UpdateBackupRPC fails in join: %v", succ.Addr, err)
		return false
	}
	//logrus.Infof("Join: Node [%s] (Id = %v)", node.Addr.Addr, node.Addr.Id)
	node.maintain()
	return true
}

func (node *Node) Ping(addr string) bool {
	err := node.remoteCall(addr, "Node.PingRPC", struct{}{}, nil)
	return err == nil
}

func (node *Node) Put(key string, value string) bool {
	var succ NodeInfo
	err := node.FindSuccRPC(getHash(key), &succ)
	if err != nil {
		//logrus.Errorf("[%s] Cannot find successor in Put (key = %v): %v", node.Addr.Addr, key, err)
		return false
	}
	err = node.remoteCall(succ.Addr, "Node.PutPairRPC", Pair{key, value}, nil)
	if err != nil {
		//logrus.Errorf("[%s] PutPairRPC fails in Put (key = %v, val = %v): %v", node.Addr.Addr, key, value, err)
		return false
	}
	err = node.remoteCall(succ.Addr, "Node.UpdateBackupRPC", struct{}{}, nil)
	if err != nil {
		//logrus.Errorf("[%s] UpdateBackupRPC fails in Put (key = %v, val = %v): %v", node.Addr.Addr, key, value, err)
		return false
	}
	//logrus.Infof("[%s] Put succeeds (key = %s, value = %s) in node %s", node.Addr.Addr, key, value, succ.Addr)
	return true
}

func (node *Node) Get(key string) (ok bool, val string) {
	var succ NodeInfo
	node.FindSuccRPC(getHash(key), &succ)
	err := node.remoteCall(succ.Addr, "Node.GetPairRPC", key, &val)
	if err != nil {
		ok = false
		val = ""
		//logrus.Errorf("[%s] Cannot get data in Get (key = %v): %v", node.Addr.Addr, key, err)
	} else {
		ok = true
	}
	return
}

func (node *Node) Delete(key string) bool {
	var succ NodeInfo
	err := node.FindSuccRPC(getHash(key), &succ)
	if err != nil {
		//logrus.Errorf("[%s] Cannot find successor in Delete (key = %v): %v", node.Addr.Addr, key, err)
		return false
	}
	err = node.remoteCall(succ.Addr, "Node.DeletePairRPC", key, nil)
	if err != nil {
		//logrus.Errorf("[%s] DeletePairRPC fails in Delete (key = %v): %v", node.Addr.Addr, key, err)
		return false
	}
	err = node.remoteCall(succ.Addr, "Node.UpdateBackupRPC", struct{}{}, nil)
	if err != nil {
		//logrus.Errorf("[%s] UpdateBackupRPC fails in Delete (key = %v): %v", node.Addr.Addr, key, err)
		return false
	}
	//logrus.Errorf("[%s] Delete succeeds (key = %s) in node %s", node.Addr.Addr, key, succ.Addr)
	return true
}

func (node *Node) Quit() {
	if !node.online {
		return
	}
	//fmt.Printf("[%s] starts quiting\n", node.Addr.Addr)
	//logrus.Infof("[%s] starts quiting", node.Addr.Addr)
	node.quitLock.Lock()
	defer node.quitLock.Unlock()
	node.fixSuccList()
	node.succLock.RLock()
	succ := node.succList[0]
	succList := node.succList
	node.succLock.RUnlock()
	if succ.Addr == node.Addr.Addr { // Only one node in the network
		node.online = false
		node.listener.Close()
		close(node.quit)
		//logrus.Infof("Quit: Node [%s]", node.Addr.Addr)
		return
	} else {
		node.remoteCall(succ.Addr, "Node.LockQuitLockRPC", struct{}{}, nil)
	}
	node.predLock.RLock()
	pred := node.pred
	node.predLock.RUnlock()
	if !node.Ping(pred.Addr) {
		node.FindPredRPC(node.Addr.Id, &pred)
	}
	if pred.Addr != succ.Addr {
		node.remoteCall(pred.Addr, "Node.LockQuitLockRPC", struct{}{}, nil)
	}
	err := node.remoteCall(pred.Addr, "Node.SetSuccListRPC", succList, nil)
	if err != nil {
		//logrus.Errorf("[%s] Set succList error in quit: %v", node.Addr.Addr, err)
	}
	node.dataLock.Lock()
	err = node.remoteCall(succ.Addr, "Node.AddDataRPC", &node.data, nil)
	node.data = make(map[string]string)
	node.dataLock.Unlock()
	if err != nil {
		//logrus.Errorf("[%s] Add data to successor fails in quit: %v", node.Addr.Addr, err)
	}
	err = node.remoteCall(succ.Addr, "Node.UpdateBackupRPC", struct{}{}, nil)
	if err != nil {
		//logrus.Errorf("[%s] successor UpdateBackupRPC fails in quit: %v", node.Addr.Addr, err)
	}
	if pred.Addr != succ.Addr {
		err = node.remoteCall(pred.Addr, "Node.UpdateBackupRPC", struct{}{}, nil)
		if err != nil {
			//logrus.Errorf("[%s] predeccessor UpdateBackupRPC fails in quit: %v", node.Addr.Addr, err)
		}
	}
	node.online = false
	node.listener.Close()
	close(node.quit)
	if pred.Addr != succ.Addr {
		node.remoteCall(pred.Addr, "Node.UnlockQuitLockRPC", struct{}{}, nil)
	}
	if succ.Addr != node.Addr.Addr {
		node.remoteCall(succ.Addr, "Node.UnlockQuitLockRPC", struct{}{}, nil)
	}
	//logrus.Infof("Quit: Node [%s]", node.Addr.Addr)
}

func (node *Node) ForceQuit() {
	if !node.online {
		return
	}
	//fmt.Printf("[%s] starts force quiting\n", node.Addr.Addr)
	//logrus.Infof("[%s] Force Quit begins", node.Addr.Addr)
	node.online = false
	close(node.quit)
	node.listener.Close()
	//logrus.Infof("[%s] Force Quit ends", node.Addr.Addr)
}
