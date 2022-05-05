package gossip

type Address struct {
	Network string
	Address string
}

type VersionedValue struct {
	Version uint64
	Value   string
}

type Info map[string]VersionedValue

func (i Info) StatesGreaterThan(version uint64) Info {
	fresher := make(Info)
	for key, value := range i {
		if value.Version > version {
			fresher[key] = value
		}
	}
	return fresher
}

func (i Info) Clone() Info {
	clone := Info{}
	for key, value := range i {
		clone[key] = value
	}
	return clone
}

func (i Info) MaxVersion() uint64 {
	max := uint64(0)
	for _, value := range i {
		if max < value.Version {
			max = value.Version
		}
	}
	return max
}

type Snapshot map[Address]Info

func (snapshot Snapshot) Clone() Snapshot {
	clone := Snapshot{}
	for id, m := range snapshot {
		clone[id] = m.Clone()
	}
	return clone
}
