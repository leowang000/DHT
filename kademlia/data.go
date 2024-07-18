package kademlia

import (
	"sync"
	"time"
)

const (
	republishTime = 1200 * time.Second
	abandonTime   = 2400 * time.Second
)

type KVPair struct {
	Key   string
	Value string
}

type DataInfo struct {
	value         string
	republishTime time.Time
	abandonTime   time.Time
}

type Data struct {
	data     map[string]DataInfo
	dataLock sync.RWMutex
}

func (data *Data) Init() {
	data.dataLock.Lock()
	data.data = make(map[string]DataInfo)
	data.dataLock.Unlock()
}

func (data *Data) abandon() {
	abandonList := []string{}
	data.dataLock.Lock()
	for k, v := range data.data {
		if time.Now().After(v.abandonTime) {
			abandonList = append(abandonList, k)
		}
	}
	for _, k := range abandonList {
		delete(data.data, k)
	}
	data.dataLock.Unlock()
}

func (data *Data) filter(cond func(DataInfo) bool) []KVPair {
	res := []KVPair{}
	data.dataLock.RLock()
	for k, v := range data.data {
		if cond(v) {
			res = append(res, KVPair{k, v.value})
		}
	}
	data.dataLock.RUnlock()
	return res
}

func (data *Data) putPair(key string, val string) {
	data.dataLock.Lock()
	data.data[key] = DataInfo{val, time.Now().Add(republishTime), time.Now().Add(abandonTime)}
	data.dataLock.Unlock()
}

func (data *Data) getPair(key string) (bool, string) {
	data.dataLock.RLock()
	v, ok := data.data[key]
	data.dataLock.RUnlock()
	return ok, v.value
}
