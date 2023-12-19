package main

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Broadcaster struct {
	values_seen []int
	neighbors []string
	msgs_seen []int
	node *maelstrom.Node
	mu sync.Mutex
}

func (b *Broadcaster) SetValue(v int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.values_seen = append(b.values_seen, v)
}

func (b *Broadcaster) SetTopology(t []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.neighbors = t
}

func RandomID() (int, error) {
	k := make([]byte, 8)
	_, err := rand.Read(k)
	if err != nil {
		log.Fatal(err)
		return 0, err
	}
	return int(binary.BigEndian.Uint32(k)), nil
}

func (b *Broadcaster) RememberMessageID(v int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.msgs_seen = append(b.msgs_seen, v)
}

func (b *Broadcaster) MulticastSend(msg interface{}) {
	for _, node := range b.neighbors {
		b.node.Send(node, msg)
	}
}

func (b *Broadcaster) PropagateValue(v int, id *int) {
	if b.node == nil {
		panic("me is nil")
	}

	msg := make(map[string]any)
	msg["type"] = "gossip"
	msg["value"] = v

	if id != nil {
		msg["msg_id"] = *id
	} else {
		r, err := RandomID()
		if err != nil {
			log.Fatal(err)
		}
		msg["msg_id"] = r
		b.RememberMessageID(r)
	}

	b.MulticastSend(msg)
}

func (b *Broadcaster) SeenGossipMessage(msg_id int) bool {
	for _, id := range b.msgs_seen {
		if msg_id == id {
			return true
		}
	}
	return false
}

func GetBroadcastValue(msg_body []byte) (int, error) {
	var body map[string]any
	if err := json.Unmarshal(msg_body, &body); err != nil {
		return 0, err
	}
	return int(body["message"].(float64)), nil
}

func (b *Broadcaster) HandleTopologyMessage(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	response := make(map[string]any)
	response["type"] = "topology_ok"

	ns, ok := body["topology"].(map[string]interface{})[b.node.ID()].([]interface{})
	if len(ns) <= 0 || !ok {
		return b.node.Reply(msg, response)
	}

	s := make([]string, len(ns))
	for i, v := range ns {
		s[i] = fmt.Sprint(v)
	}
	b.SetTopology(s)
	return b.node.Reply(msg, response)
}

func (b *Broadcaster) HandleReadMessage(msg maelstrom.Message) error {
	body := make(map[string]any)
	body["type"] = "read_ok"

	b.mu.Lock()
	defer b.mu.Unlock()
	body["messages"] = b.values_seen

	return b.node.Reply(msg, body)
}

func (b *Broadcaster) HandleBroadcastMessage(msg maelstrom.Message) error {
	response := make(map[string]any)
	response["type"] = "broadcast_ok"
	v, err := GetBroadcastValue(msg.Body)
	if err != nil {
		return err
	}

	b.SetValue(int(v))
	b.PropagateValue(int(v), nil)

	return b.node.Reply(msg, response)
}

func (b *Broadcaster) HandleGossipMessage(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		log.Printf("error unmarshalling message %v\n", msg)
		return err
	}

	msg_id := int(body["msg_id"].(float64))
	if b.SeenGossipMessage(msg_id) {
		return nil
	}

	if v, ok := body["value"].(float64); ok {
		b.SetValue(int(v))
		b.RememberMessageID(msg_id)
		b.MulticastSend(msg.Body)
		return nil
	}

	return nil
}

func main() {
	n := maelstrom.NewNode()
	broadcaster := Broadcaster{}
	broadcaster.node = n
	broadcaster.values_seen = make([]int, 0)
	broadcaster.msgs_seen = make([]int, 0)

	n.Handle("broadcast", broadcaster.HandleBroadcastMessage)
	n.Handle("read", broadcaster.HandleReadMessage)
	n.Handle("topology", broadcaster.HandleTopologyMessage)
	n.Handle("gossip", broadcaster.HandleGossipMessage)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
