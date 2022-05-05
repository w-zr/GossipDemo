package gossip

type Address struct {
	Network string
	Address string
}

type Info map[string]string

func (i Info) Clone() Info {
	clone := Info{}
	for key, value := range i {
		clone[key] = value
	}
	return clone
}

type Snapshot map[Address]Info

func (snapshot Snapshot) Clone() Snapshot {
	clone := Snapshot{}
	for id, m := range snapshot {
		clone[id] = m.Clone()
	}
	return clone
}
