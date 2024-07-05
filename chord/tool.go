package chord

import (
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

// calculate (n + 2 ^ i) % (2 ^ 160)
func add(n *big.Int, i int) *big.Int {
	res := new(big.Int).Add(n, exp[i])
	if res.Cmp(exp[160]) >= 0 {
		res.Sub(res, exp[160])
	}
	return res
}

func belong(leftClosed bool, rightClosed bool, beg *big.Int, end *big.Int, num *big.Int) bool {
	cmpBegEnd, cmpNumBeg, cmpNumEnd := beg.Cmp(end), num.Cmp(beg), num.Cmp(end)
	switch cmpBegEnd {
	case -1:
		if cmpNumBeg == 1 && cmpNumEnd == -1 {
			return true
		} else if cmpNumBeg == -1 || cmpNumEnd == 1 {
			return false
		} else if cmpNumBeg == 0 {
			return leftClosed
		} else if cmpNumEnd == 0 {
			return rightClosed
		}
	case 1:
		if cmpNumBeg == 1 || cmpNumEnd == -1 {
			return true
		} else if cmpNumBeg == -1 && cmpNumEnd == 1 {
			return false
		} else if cmpNumBeg == 0 {
			return leftClosed
		} else if cmpNumEnd == 0 {
			return rightClosed
		}
	case 0:
		if cmpNumBeg == 0 {
			return leftClosed || rightClosed
		} else {
			return true
		}
	}
	return false
}
