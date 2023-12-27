package main

import (
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type AddMessage struct {
	Type string
	Delta int
}

type ReadMessage struct {
	Type string
}

type GossipMessage struct {
	Type string
	Counters map[string]int
}

type InitMessage struct {
	Type string `json:"type"`
	NodeID string `json:"node_id"`
	NodeIDs []string `json:"node_ids"`
	MsgID int `json:"msg_id"`
}

type GCounter struct {
	// maps node ids to counters.
	counters map[string]int
	// true if we've updated counters but have yet to propagate state, false otherwise.
	state_dirty bool
	neighbors []string
	node *maelstrom.Node
	//kv *maelstrom.KV
	mu sync.Mutex
}

func (gc *GCounter) NewGossipMessage() GossipMessage {
	msg := GossipMessage{}
	msg.Type = "gossip"
	gc.mu.Lock()
	msg.Counters = gc.counters
	gc.mu.Unlock()
	return msg
}

func (gc *GCounter) PropagateState() {
	msg := gc.NewGossipMessage()
	gc.Multicast(msg)
	gc.mu.Lock()
	gc.state_dirty = false
	gc.mu.Unlock()
}

func (gc *GCounter) Multicast(msg interface{}) {
	gc.mu.Lock()
	if gc.node == nil {
		panic("me is nil")
	}
	for _, node := range gc.neighbors {
		if node != gc.node.ID() {
			err := gc.node.RPC(node, msg, gc.HandleGossipResponse)
			if err != nil {
				log.Printf("problem sending message to node %v\n", node)
			}
		}
	}
	gc.mu.Unlock()
}

func (gc *GCounter) HandleReadMessage(raw_msg maelstrom.Message) error {
	msg := ReadMessage{}
	if err := json.Unmarshal(raw_msg.Body, &msg); err != nil {
		
		return err
	}
	if msg.Type != "read" {
		return errors.New("HandleReadMessage tried to process non-read message!")
	}
	v := gc.Value()
	response := make(map[string]any)
	response["type"] = "read_ok"
	response["value"] = v
	return gc.node.Reply(raw_msg, response)
}

func (gc *GCounter) HandleAddMessage(raw_msg maelstrom.Message) error {
	msg := AddMessage{}
	if err := json.Unmarshal(raw_msg.Body, &msg); err != nil {
		return err
	}
	if msg.Type != "add" {
		return errors.New("HandleAddMessage tried to process non-add message!")
	}

	gc.Add(msg.Delta)
	response := make(map[string]any)
	response["type"] = "add_ok"
	return gc.node.Reply(raw_msg, response)
}

func (gc *GCounter) HandleGossipMessage(msg maelstrom.Message) error {
	body := GossipMessage{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {		
		return err
	}
	
	if err := gc.Merge(body.Counters); err != nil {
		return err
	}

	reply := make(map[string]any)
	reply["type"] = "gossip_ok"

	return gc.node.Reply(msg, reply)
}

func (gc *GCounter) HandleGossipResponse(msg maelstrom.Message) error {
	body := GossipMessage{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	log.Printf("node %v recv'd gossip response from node %v\n", msg.Dest, msg.Src)
	return nil
}

func (gc *GCounter) HandleNodeInit(raw_msg maelstrom.Message) error {
	init_msg := InitMessage{}
	if err := json.Unmarshal(raw_msg.Body, &init_msg); err != nil {
		return err
	}

	gc.mu.Lock()
	defer gc.mu.Unlock()

	gc.neighbors = init_msg.NodeIDs
	log.Println(gc.neighbors)

	response := make(map[string]any)
	response["type"] = "init_ok"
	return gc.node.Reply(raw_msg, response)
}

func (gc *GCounter) Compare(other map[string]int) bool {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	for k := range gc.counters {
		if gc.counters[k] > other[k] {
			return false
		}
	}

	return true
}

func (gc *GCounter) Merge(other map[string]int) error {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	if len(other) != len(gc.counters) {
		return errors.New("len(other) != len(gc.counters)")
	}

	log.Printf("node %v pre-merge: %v\n", gc.node.ID(), gc.counters)

	for k := range gc.counters {
		gc.counters[k] = max(gc.counters[k], other[k])
	}

	log.Printf("node %v post-merge: %v\n", gc.node.ID(), gc.counters)

	return nil
}

func (gc *GCounter) Add(k int) {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	gc.counters[gc.node.ID()] += k
	log.Printf("node %v added %d -- it is now %d\n", gc.node.ID(), k, gc.counters[gc.node.ID()])
	gc.state_dirty = true
}

func (gc *GCounter) Value() int {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	v := 0
	for k := range gc.counters {
		v += gc.counters[k]
	}
	return v
}

func (gc *GCounter) HandleTicker() {
	gc.mu.Lock()
	if gc.state_dirty {
		gc.mu.Unlock()
		gc.PropagateState()
	}
}

func main() {
	gc := GCounter{}
	gc.node = maelstrom.NewNode()
	//gc.kv = maelstrom.NewSeqKV(gc.node)
	gc.counters = make(map[string]int)
	gc.neighbors = make([]string, 8)

	gc.node.Handle("init", gc.HandleNodeInit)
	gc.node.Handle("add", gc.HandleAddMessage)
	gc.node.Handle("read", gc.HandleReadMessage)
	gc.node.Handle("gossip", gc.HandleGossipMessage)

	ticker := time.NewTicker(1 * time.Second)
    done := make(chan bool)
    go func() {
        for {
            select {
            case <-done:
                return
            case _ = <-ticker.C:
				gc.HandleTicker()
            }
        }
    }()

	if err := gc.node.Run(); err != nil {
		log.Fatal(err)
	}
}
