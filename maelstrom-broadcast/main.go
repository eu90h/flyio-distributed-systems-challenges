package main

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Broadcaster struct {
	// this maps (value, node) pairs to a bool based on whether or not a response was received.
	received_response map[int]map[string]bool
	//map values to message tags
	values_seen map[int]int
	//map message tags to values
	msg_ids_seen map[int]int
	values []int
	neighbors []string
	node *maelstrom.Node
	mu sync.Mutex
}

func RandomID() (int, error) {
	k := make([]byte, 4)
	_, err := rand.Read(k)
	if err != nil {
		return 0, err
	}
	return int(binary.BigEndian.Uint32(k)), nil
}

func (b *Broadcaster) Multicast(msg interface{}) {
	b.MulticastExclude(msg, "")
}

func (b *Broadcaster) MulticastExclude(msg interface{}, excluded_node string) {
	b.mu.Lock()
	if b.node == nil {
		panic("me is nil")
	}
	for _, node := range b.neighbors {
		if node != excluded_node {
			err := b.node.RPC(node, msg, b.HandleGossipResponse)
			if err != nil {
				log.Printf("problem sending message to node %v\n", node)
			}
		}
	}
	b.mu.Unlock()
}

func (b *Broadcaster) PropagateValue(v int) int {
	msg := make(map[string]any)
	msg["type"] = "gossip"
	msg["message"] = v
	r, err := RandomID()
	if err != nil {
		log.Fatal(err)
	}
	msg["tag"] = r

	b.mu.Lock()
	if b.received_response[v] == nil {
		b.received_response[v] = make(map[string]bool)
	}
	for _, node := range b.neighbors {
		b.received_response[v][node] = false
	}
	b.msg_ids_seen[r] = v
	b.values_seen[v] = r
	b.mu.Unlock()
	b.Multicast(msg)
	return r
}

func (b *Broadcaster) HandleBroadcastMessage(msg maelstrom.Message) error {
	body := make(map[string]any)
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	message_value := int(body["message"].(float64))

	b.mu.Lock()
	if _, ok := b.values_seen[message_value]; ok {
		return nil
	}
	b.values = append(b.values, message_value)
	b.mu.Unlock()

	id := b.PropagateValue(message_value)

	b.mu.Lock()
	b.values_seen[message_value] = id
	b.mu.Unlock()

	reply := make(map[string]any)
	reply["type"] = "broadcast_ok"
	return b.node.Reply(msg, reply)
}

func (b *Broadcaster) HandleGossipResponse(msg maelstrom.Message) error {
	body := make(map[string]any)
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	if _, ok := body["message"].(float64); !ok {
		return nil
	}
	v := int(body["message"].(float64))

	b.mu.Lock()
	if b.received_response[v] == nil {
		b.received_response[v] = make(map[string]bool)
	}
	b.received_response[v][msg.Src] = true
	b.mu.Unlock()

	return nil
}

func (b *Broadcaster) HandleReadMessage(msg maelstrom.Message) error {
	body := make(map[string]any)
	body["type"] = "read_ok"

	b.mu.Lock()
	body["messages"] = b.values
	b.mu.Unlock()

	return b.node.Reply(msg, body)
}

func GetNeighbors(msg maelstrom.Message, node_id string) ([]string, error) {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return nil, err
	}

	ns, ok := body["topology"].(map[string]interface{})[node_id].([]interface{})
	if len(ns) < 0 || !ok {
		return nil, errors.New("problem parsing topology message")
	}

	s := make([]string, len(ns))
	for i, v := range ns {
		s[i] = fmt.Sprint(v)
	}

	return s, nil
}

func (b *Broadcaster) HandleTopologyMessage(msg maelstrom.Message) error {
	b.mu.Lock()
	neighbors, err := GetNeighbors(msg, b.node.ID())
	if err != nil {
		return err
	}
	b.neighbors = neighbors
	b.mu.Unlock()

	reply := make(map[string]any)
	reply["type"] = "topology_ok"

	return b.node.Reply(msg, reply)
}

func (b *Broadcaster) HandleGossipMessage(msg maelstrom.Message) error {
	body := make(map[string]any)
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	message_value := int(body["message"].(float64))
	message_tag := int(body["tag"].(float64))

	b.mu.Lock()
	if _, ok := b.values_seen[message_value]; ok {
		b.mu.Unlock()
		reply := make(map[string]any)
		reply["type"] = "gossip_ok"
		reply["message"] = body["message"]
		
		return b.node.Reply(msg, reply)
	}
	_, ok := b.msg_ids_seen[message_tag]
	if ok {
		reply := make(map[string]any)
		reply["type"] = "gossip_ok"
		b.mu.Unlock()
		return b.node.Reply(msg, reply)
	}
	b.msg_ids_seen[message_tag] = message_value
	b.values = append(b.values, message_value)
	b.values_seen[message_value] = message_tag
	b.mu.Unlock()
	b.MulticastExclude(body, msg.Src)

	reply := make(map[string]any)
	reply["type"] = "gossip_ok"
	reply["message"] = body["message"]

	return b.node.Reply(msg, reply)
}

func (b *Broadcaster) HandleTicker() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, v := range b.values {
		msg := make(map[string]any)
		msg["type"] = "gossip"
		msg["message"] = v
		msg["tag"] = b.values_seen[v]
		for _, node := range b.neighbors {
			if node != b.node.ID() && !b.received_response[v][node] {
				log.Printf("node %v hasn't seen %v\n", node, v)
				err := b.node.RPC(node, msg, b.HandleGossipResponse)
				if err != nil {
					log.Printf("problem sending message to node %v\n", node)
				}
			}
		}
	}
}

func main() {
	n := maelstrom.NewNode()
	broadcaster := Broadcaster{}
	broadcaster.node = n
	broadcaster.msg_ids_seen = make(map[int]int)
	broadcaster.values = make([]int, 0)
	broadcaster.neighbors = make([]string, 0)
	broadcaster.received_response = make(map[int]map[string]bool)
	broadcaster.values_seen = make(map[int]int)

	n.Handle("broadcast", broadcaster.HandleBroadcastMessage)
	n.Handle("read", broadcaster.HandleReadMessage)
	n.Handle("topology", broadcaster.HandleTopologyMessage)
	n.Handle("gossip", broadcaster.HandleGossipMessage)

	ticker := time.NewTicker(5 * time.Millisecond)
    done := make(chan bool)
    go func() {
        for {
            select {
            case <-done:
                return
            case _ = <-ticker.C:
				broadcaster.HandleTicker()
            }
        }
    }()
	
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
