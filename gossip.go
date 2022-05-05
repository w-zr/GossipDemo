package gossip

import (
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"gossip/codec"
)

const (
	fanOut         = 1
	gossipInterval = time.Millisecond * 50
)

type Gossip struct {
	localAddress Address

	mu       sync.RWMutex
	snapshot Snapshot
	version  uint64

	seedNodes []string
	peers     []*client
}

func NewServer(network, address string, seedNodes []string) *Gossip {
	s := &Gossip{
		localAddress: Address{
			Network: network,
			Address: address,
		},

		snapshot: make(Snapshot),
		version:  1,

		peers: make([]*client, 0),
	}

	s.seedNodes = s.removeSelfAddress(seedNodes)
	return s
}

func (s *Gossip) GetSnapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot.Clone()
}

func (s *Gossip) removeSelfAddress(seedNodes []string) []string {
	seeds := make([]string, 0)
	for _, seed := range seedNodes {
		if seed != s.localAddress.Address {
			seeds = append(seeds, seed)
		}
	}
	return seeds
}

func (s *Gossip) AddLocalState(key string, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.snapshot[s.localAddress]; !ok {
		s.snapshot[s.localAddress] = make(Info)
	}
	s.snapshot[s.localAddress][key] = value
}

func (s *Gossip) Start() {
	l, err := net.Listen(s.localAddress.Network, s.localAddress.Address)
	if err != nil {
		log.Fatal("Network error:", err)
	}
	log.Println("start gossip server on", l.Addr())
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				log.Println("gossip server: accept error:", err)
				return
			}
			go s.handleGossipRequest(conn)
		}
	}()
	go s.startGossip()
}

func (s *Gossip) startGossip() {
	s.doGossip()
	ticker := time.NewTicker(gossipInterval)
	for {
		<-ticker.C
		s.doGossip()
	}
}

func (s *Gossip) handleGossipRequest(conn net.Conn) {
	f := codec.NewCodecFuncMap[codec.GobType]
	cc := f(conn)
	for {
		snapshot := make(Snapshot)
		err := cc.Read(&snapshot)
		if err != nil {
			log.Fatal("gossip server: read failed")
			return
		}
		diff := delta(s.GetSnapshot(), snapshot)
		err = cc.Write(&diff)
		if err != nil {
			log.Fatal("gossip server: write failed")
			return
		}
		s.merge(snapshot)
	}
}

func (s *Gossip) doGossip() {
	knownNodes := s.liveNodes()
	if len(knownNodes) == 0 {
		s.sendGossip(s.seedNodes, fanOut)
	} else {
		s.sendGossip(knownNodes, fanOut)
	}
}

func (s *Gossip) liveNodes() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var liveNodes []string
	for key := range s.snapshot {
		liveNodes = append(liveNodes, key.Address)
	}
	return s.removeSelfAddress(liveNodes)
}

func (s *Gossip) sendGossip(targets []string, fanOut int) {
	if len(targets) == 0 {
		return
	}

	for i := 0; i < fanOut; i++ {
		r := rand.Intn(len(targets))
		if err := s.sendGossipTo(targets[r]); err != nil {
			for i, c := range s.peers {
				if c.remote.Address == targets[r] {
					s.peers = append(s.peers[:i], s.peers[i+1:]...)
				}
			}
		}
	}
}

func (s *Gossip) sendGossipTo(target string) error {
	var peer *client
	for _, c := range s.peers {
		if c.remote.Address == target {
			peer = c
		}
	}
	if peer == nil {
		var err error
		peer, err = Dial(s.localAddress.Network, target)
		if err != nil {
			return err
		}
		peer.server = s
		peer.remote = Address{s.localAddress.Network, target}
		s.peers = append(s.peers, peer)
		go peer.receive()
	}

	peer.send(s.GetSnapshot())
	return nil
}

func delta(from, to Snapshot) Snapshot {
	diff := make(Snapshot)
	for addr := range from {
		if _, ok := to[addr]; !ok {
			diff[addr] = from[addr]
			continue
		}
		fromMap := from[addr]
		toMap := to[addr]
		diffMap := make(Info)
		for key, value := range fromMap {
			if _, ok := toMap[key]; !ok {
				diffMap[key] = value
				continue
			}
		}
		if len(diffMap) != 0 {
			diff[addr] = diffMap
		}
	}
	return diff
}

func (s *Gossip) merge(other Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot == nil {
		s.snapshot = make(Snapshot)
	}
	diff := delta(other, s.snapshot)
	for addr, diffValue := range diff {
		if _, ok := s.snapshot[addr]; !ok {
			s.snapshot[addr] = diffValue
		} else {
			for key, value := range diffValue {
				s.snapshot[addr][key] = value
			}
		}
	}
}
