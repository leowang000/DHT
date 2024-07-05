package test

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"math/rand"
	"sync"
	"time"
)

func getHash(ip string) *big.Int {
	hash := sha1.Sum([]byte(ip))
	hashInt := new(big.Int)
	return hashInt.SetBytes(hash[:])
}

func basicTest() (bool, int, int) {
	basicFailedCnt, basicTotalCnt, panicked := 0, 0, false

	defer func() {
		if r := recover(); r != nil {
			red.Println("Program panicked with", r)
		}
		panicked = true
	}()

	nodes := new([basicTestNodeSize + 1]dhtNode)
	nodeAddresses := new([basicTestNodeSize + 1]string)
	kvMap := make(map[string]string)

	/* "Run" all nodes. */
	wg = new(sync.WaitGroup)
	for i := 0; i <= basicTestNodeSize; i++ {
		nodes[i] = NewNode(firstPort + i)
		nodeAddresses[i] = portToAddr(localAddress, firstPort+i)

		wg.Add(1)
		go nodes[i].Run()
	}

	nodesInNetwork := make([]int, 0, basicTestNodeSize+1)

	time.Sleep(basicTestAfterRunSleepTime)

	/* Node 0 now creates a new network. */
	nodes[0].Create()
	nodesInNetwork = append(nodesInNetwork, 0)

	/* 5 rounds in total. */
	nextJoinNode := 1
	for t := 1; t <= basicTestRoundNum; t++ {
		cyan.Printf("Basic Test Round %d\n", t)

		/* Join. */
		joinInfo := testInfo{
			msg:       fmt.Sprintf("Join (round %d)", t),
			failedCnt: 0,
			totalCnt:  0,
		}
		cyan.Printf("Start joining (round %d)\n", t)
		for j := 1; j <= basicTestRoundJoinNodeSize; j++ {
			addr := nodeAddresses[nodesInNetwork[rand.Intn(len(nodesInNetwork))]]
			if !nodes[nextJoinNode].Join(addr) {
				joinInfo.fail()
			} else {
				joinInfo.success()
			}
			nodesInNetwork = append(nodesInNetwork, nextJoinNode)

			time.Sleep(basicTestJoinQuitSleepTime)
			nextJoinNode++
		}
		joinInfo.finish(&basicFailedCnt, &basicTotalCnt)

		time.Sleep(basicTestAfterJoinQuitSleepTime)

		// for i := 0; i <= 100; i++ {
		// 	pred, succ := nodes[i].GetPred(), nodes[i].GetSucc()
		// 	fmt.Printf("[%v] pred = %s, succ = %s\n", i, pred, succ)
		// }
		// fmt.Printf("\n")

		/* Put, part 1. */
		put1Info := testInfo{
			msg:       fmt.Sprintf("Put (round %d, part 1)", t),
			failedCnt: 0,
			totalCnt:  0,
		}
		cyan.Printf("Start putting (round %d, part 1)\n", t)
		for i := 1; i <= basicTestRoundPutSize; i++ {
			key := randString(lengthOfKeyValue)
			value := randString(lengthOfKeyValue)
			kvMap[key] = value

			if !nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Put(key, value) {
				put1Info.fail()
			} else {
				put1Info.success()
			}
		}
		put1Info.finish(&basicFailedCnt, &basicTotalCnt)

		/* Get, part 1. */
		get1Info := testInfo{
			msg:       fmt.Sprintf("Get (round %d, part 1)", t),
			failedCnt: 0,
			totalCnt:  0,
		}
		cyan.Printf("Start getting (round %d, part 1)\n", t)
		get1Cnt := 0
		for key, value := range kvMap {
			tmp := nodesInNetwork[rand.Intn(len(nodesInNetwork))]
			ok, res := nodes[tmp].Get(key)
			if !ok || res != value {
				//fmt.Printf("[%v] %v %s %v %s %s\n", tmp, ok, key, getHash(key), res, value)
				get1Info.fail()
			} else {
				get1Info.success()
			}

			get1Cnt++
			if get1Cnt == basicTestRoundGetSize {
				break
			}
		}
		get1Info.finish(&basicFailedCnt, &basicTotalCnt)

		/* Delete, part 1. */
		delete1Info := testInfo{
			msg:       fmt.Sprintf("Delete (round %d, part 1)", t),
			failedCnt: 0,
			totalCnt:  0,
		}
		cyan.Printf("Start deleting (round %d, part 1)\n", t)
		for i := 1; i <= basicTestRoundDeleteSize; i++ {
			for key := range kvMap {
				tmp := nodesInNetwork[rand.Intn(len(nodesInNetwork))]
				delete(kvMap, key)
				success := nodes[tmp].Delete(key)
				if !success {
					//fmt.Printf("[%v] %s %v\n", tmp, key, getHash(key))
					delete1Info.fail()
				} else {
					delete1Info.success()
				}
				break
			}
		}
		delete1Info.finish(&basicFailedCnt, &basicTotalCnt)

		/* Quit. */
		cyan.Printf("Start quitting (round %d)\n", t)
		for i := 1; i <= basicTestRoundQuitNodeSize; i++ {
			idxInArray := rand.Intn(len(nodesInNetwork))

			nodes[nodesInNetwork[idxInArray]].Quit()
			nodesInNetwork = removeFromArray(nodesInNetwork, idxInArray)

			time.Sleep(basicTestJoinQuitSleepTime)
		}
		green.Printf("Quit (round %d) passed.\n", t)
		time.Sleep(basicTestAfterJoinQuitSleepTime)

		/* Put, part 2. */
		put2Info := testInfo{
			msg:       fmt.Sprintf("Put (round %d, part 2)", t),
			failedCnt: 0,
			totalCnt:  0,
		}
		cyan.Printf("Start putting (round %d, part 2)\n", t)
		for i := 1; i <= basicTestRoundPutSize; i++ {
			key := randString(lengthOfKeyValue)
			value := randString(lengthOfKeyValue)
			kvMap[key] = value

			if !nodes[nodesInNetwork[rand.Intn(len(nodesInNetwork))]].Put(key, value) {
				put2Info.fail()
			} else {
				put2Info.success()
			}
		}
		put2Info.finish(&basicFailedCnt, &basicTotalCnt)

		/* Get, part 2. */
		get2Info := testInfo{
			msg:       fmt.Sprintf("Get (round %d, part 2)", t),
			failedCnt: 0,
			totalCnt:  0,
		}
		cyan.Printf("Start getting (round %d, part 2)\n", t)
		get2Cnt := 0
		for key, value := range kvMap {
			tmp := nodesInNetwork[rand.Intn(len(nodesInNetwork))]
			ok, res := nodes[tmp].Get(key)
			if !ok || res != value {
				//fmt.Printf("[%v] %v %s %v %s %s\n", tmp, ok, key, getHash(key), res, value)
				get2Info.fail()
			} else {
				get2Info.success()
			}

			get2Cnt++
			if get2Cnt == basicTestRoundGetSize {
				break
			}
		}
		get2Info.finish(&basicFailedCnt, &basicTotalCnt)

		/* Delete, part 2. */
		delete2Info := testInfo{
			msg:       fmt.Sprintf("Delete (round %d, part 2)", t),
			failedCnt: 0,
			totalCnt:  0,
		}
		cyan.Printf("Start deleting (round %d, part 2)\n", t)
		for i := 1; i <= basicTestRoundDeleteSize; i++ {
			for key := range kvMap {
				delete(kvMap, key)
				tmp := nodesInNetwork[rand.Intn(len(nodesInNetwork))]
				success := nodes[tmp].Delete(key)
				if !success {
					//fmt.Printf("[%v] %s %v\n", tmp, key, getHash(key))
					delete2Info.fail()
				} else {
					delete2Info.success()
				}
				break
			}
		}
		delete2Info.finish(&basicFailedCnt, &basicTotalCnt)

		// for i := 0; i <= 100; i++ {
		// 	pred, succ := nodes[i].GetPred(), nodes[i].GetSucc()
		// 	fmt.Printf("[%v] pred = %s, succ = %s\n", i, pred, succ)
		// }
		// fmt.Printf("\n")
	}

	/* All nodes quit. */
	for i := 0; i <= basicTestNodeSize; i++ {
		nodes[i].Quit()
	}

	return panicked, basicFailedCnt, basicTotalCnt
}