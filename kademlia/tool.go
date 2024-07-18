package kademlia

import (
	"container/list"
	"crypto/sha1"
	"math/big"
)

const hashSize = 160

var exp [hashSize + 1]*big.Int

func init() {
	for i := range exp {
		exp[i] = new(big.Int).Lsh(big.NewInt(1), uint(i))
	}
}

func getHash(ip string) *big.Int {
	hash := sha1.Sum([]byte(ip))
	hashInt := new(big.Int)
	return hashInt.SetBytes(hash[:])
}

func listFilter[T any](ls *list.List, cond func(T) bool) []T {
	res := []T{}
	for it := ls.Front(); it != nil; it = it.Next() {
		if v, ok := it.Value.(T); ok {
			if cond(v) {
				res = append(res, v)
			}
		}
	}
	return res
}

func listFilterSize[T any](ls *list.List, size int, cond func(T) bool) []T {
	res := []T{}
	for it := ls.Front(); it != nil; it = it.Next() {
		if v, ok := it.Value.(T); ok {
			if cond(v) {
				res = append(res, v)
				if len(res) == size {
					return res
				}
			}
		}
	}
	return res
}

func listFind[T any](ls *list.List, val T, equal func(T, T) bool) *list.Element {
	for it := ls.Front(); it != nil; it = it.Next() {
		if v, ok := it.Value.(T); ok {
			if equal(v, val) {
				return it
			}
		}
	}
	return nil
}

func flushList(ls *list.List, other []string, cmp func(string, string) bool) bool {
	same := true
	for _, ip := range other {
		if ls.Len() == 0 {
			same = false
			ls.PushBack(ip)
			continue
		}
		elem := listFind(ls, ip, func(lhs string, rhs string) bool { return !cmp(lhs, rhs) && !cmp(rhs, lhs) })
		if elem == nil {
			same = false
			if ls.Len() > 0 && cmp(ls.Back().Value.(string), ip) {
				ls.PushBack(ip)
				continue
			}
			for it := ls.Front(); it != nil; it = it.Next() {
				if cmp(ip, it.Value.(string)) {
					ls.InsertBefore(ip, it)
					break
				}
			}
		}
	}
	return same
}

func getBucketId(nodeId *big.Int, target *big.Int) int {
	dis := new(big.Int).Xor(nodeId, target)
	for i := hashSize - 1; i >= 0; i-- {
		if dis.Cmp(exp[i]) >= 0 {
			return i
		}
	}
	return -1
}
