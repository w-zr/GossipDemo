package gossip

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"

	"gossip/codec"
)

type client struct {
	cc      codec.Codec
	sending sync.Mutex
	mu      sync.Mutex

	closing  bool
	shutdown bool

	server *Gossip
	remote Address
}

var ErrShutdown = errors.New("connection is shut down")

func NewClient(conn net.Conn) (*client, error) {
	f := codec.NewCodecFuncMap[codec.GobType]
	if f == nil {
		err := fmt.Errorf("invalid codec type")
		log.Println("rpc client: codec error:", err)
		return nil, err
	}

	return newClientCodec(f(conn)), nil
}

func newClientCodec(cc codec.Codec) *client {
	client := &client{
		cc: cc,
	}
	return client
}

func (client *client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()

	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	return client.cc.Close()
}

func (client *client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

func (client *client) receive() {
	var err error
	for err == nil {
		var h Snapshot
		if err = client.cc.Read(&h); err != nil {
			break
		}
		client.server.merge(h)
	}
	client.terminate(err)
}

func (client *client) terminate(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()

	client.shutdown = true
	log.Fatal(err)
}

func Dial(network, address string) (client *client, err error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	defer func() {
		if client == nil {
			_ = conn.Close()
		}
	}()
	return NewClient(conn)
}

func (client *client) send(content Snapshot) {
	client.sending.Lock()
	if err := client.cc.Write(content); err != nil {
		client.sending.Unlock()
		client.terminate(err)
	}
	client.sending.Unlock()
}
