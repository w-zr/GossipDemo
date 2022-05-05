package gossip

import (
	"log"
	"strconv"
	"testing"
	"time"
)

func TestGossip(t *testing.T) {
	clusterSize := 20
	servers := make([]*Gossip, clusterSize)
	IP := "127.0.0.1"
	basePort := 20000

	for i := range servers {
		addr := IP + ":" + strconv.Itoa(basePort+i)
		servers[i] = NewServer("tcp", addr, []string{"127.0.0.1:20000", "127.0.0.1:20001", "127.0.0.1:20002"})
		servers[i].Start()
	}

	servers[clusterSize-1].AddLocalState("a", "b")
	start := time.Now()
	for {
		count := 0
		for i := 0; i < clusterSize; i++ {
			if _, ok := servers[i].GetSnapshot()[Address{Network: "tcp", Address: servers[clusterSize-1].localAddress.Address}]["a"]; ok {
				count++
			}
		}
		if count == clusterSize {
			since := time.Since(start)
			log.Println("gossip successfully in", since)
			break
		}
		time.Sleep(time.Millisecond * 50)
	}
}
