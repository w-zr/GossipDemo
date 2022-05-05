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
	fanOut         = 3
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
	s.AddLocalState("key", "value")
	return s
}

func (g *Gossip) GetSnapshot() Snapshot {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.snapshot.Clone()
}

func (g *Gossip) removeSelfAddress(targets []string) []string {
	for i, target := range targets {
		if target == g.localAddress.Address {
			seeds := append(targets[:i], targets[i+1:]...)
			return seeds
		}
	}
	return targets
}

func (g *Gossip) AddLocalState(key string, value string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.snapshot[g.localAddress]; !ok {
		g.snapshot[g.localAddress] = make(Info)
	}
	g.snapshot[g.localAddress][key] = VersionedValue{g.version, value}
	g.version++
}

func (g *Gossip) Start() {
	l, err := net.Listen(g.localAddress.Network, g.localAddress.Address)
	if err != nil {
		log.Fatal("Network error:", err)
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				log.Println("gossip server: accept error:", err)
				return
			}
			go g.handleGossipRequest(conn)
		}
	}()
	go g.startGossip()
}

func (g *Gossip) startGossip() {
	g.doGossip()
	ticker := time.NewTicker(gossipInterval)
	for {
		<-ticker.C
		g.doGossip()
	}
}

func (g *Gossip) handleGossipRequest(conn net.Conn) {
	f := codec.NewCodecFuncMap[codec.GobType]
	cc := f(conn)
	for {
		maxVersions := make(map[Address]uint64)
		if err := cc.Read(&maxVersions); err != nil {
			log.Fatal("gossip server: read failed")
			return
		}

		g.mu.Lock()
		for addr := range maxVersions {
			if _, ok := g.snapshot[addr]; !ok {
				g.snapshot[addr] = make(Info)
			}
		}
		g.mu.Unlock()

		diff := g.getMissingAndNodeStatesHigherThan(maxVersions)
		if err := cc.Write(&diff); err != nil {
			log.Fatal("gossip server: write failed")
			return
		}
	}
}

func (g *Gossip) doGossip() {
	knownNodes := g.liveNodes()
	if len(knownNodes) == 0 {
		g.sendGossip(g.seedNodes, fanOut)
	} else {
		g.sendGossip(knownNodes, fanOut)
	}
}

func (g *Gossip) liveNodes() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	var liveNodes []string
	for key := range g.snapshot {
		liveNodes = append(liveNodes, key.Address)
	}
	return g.removeSelfAddress(liveNodes)
}

func (g *Gossip) sendGossip(targets []string, fanOut int) {
	if len(targets) == 0 {
		return
	}

	for i := 0; i < fanOut; i++ {
		r := rand.Intn(len(targets))
		if err := g.sendKnownVersions(targets[r]); err != nil {
			for i, c := range g.peers {
				if c.remote.Address == targets[r] {
					g.peers = append(g.peers[:i], g.peers[i+1:]...)
				}
			}
		}
	}
}

func (g *Gossip) getOrCreateClient(target string) (*client, error) {
	var peer *client
	for _, c := range g.peers {
		if c.remote.Address == target {
			peer = c
		}
	}
	if peer == nil {
		var err error
		peer, err = Dial(g.localAddress.Network, target)
		if err != nil {
			return nil, err
		}
		peer.server = g
		peer.remote = Address{g.localAddress.Network, target}
		g.peers = append(g.peers, peer)
		go peer.receive()
	}
	return peer, nil
}

func (g *Gossip) sendKnownVersions(target string) error {
	if peer, err := g.getOrCreateClient(target); err != nil {
		return err
	} else {
		peer.sendKnownVersions(g.getMaxKnownNodeVersions())
	}
	return nil
}

func (g *Gossip) getMaxKnownNodeVersions() map[Address]uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	maxKnownNodeVersions := make(map[Address]uint64)
	for addr, info := range g.snapshot {
		maxKnownNodeVersions[addr] = info.MaxVersion()
	}
	return maxKnownNodeVersions
}

func (g *Gossip) getMissingAndNodeStatesHigherThan(nodeMaxVersions map[Address]uint64) Snapshot {
	g.mu.RLock()
	defer g.mu.RUnlock()
	diff := make(Snapshot)
	for addr, maxVersion := range nodeMaxVersions {
		if nodeState, ok := g.snapshot[addr]; !ok {
			continue
		} else {
			diffState := nodeState.StatesGreaterThan(maxVersion)
			if len(diffState) != 0 {
				diff[addr] = diffState
			}
		}
	}
	for addr := range g.snapshot {
		if _, ok := nodeMaxVersions[addr]; !ok {
			diff[addr] = g.snapshot[addr].Clone()
		}
	}
	return diff
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
		for key, fromValue := range fromMap {
			if toValue, ok := toMap[key]; !ok {
				diffMap[key] = fromValue
				continue
			} else {
				if fromValue.Version > toValue.Version {
					diffMap[key] = fromValue
				}
			}
		}
		if len(diffMap) != 0 {
			diff[addr] = diffMap
		}
	}
	return diff
}

func (g *Gossip) merge(other Snapshot) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.snapshot == nil {
		g.snapshot = make(Snapshot)
	}
	diff := delta(other, g.snapshot)
	for addr, diffValue := range diff {
		if _, ok := g.snapshot[addr]; !ok {
			g.snapshot[addr] = diffValue
		} else {
			for key, value := range diffValue {
				g.snapshot[addr][key] = value
			}
		}
	}
}
