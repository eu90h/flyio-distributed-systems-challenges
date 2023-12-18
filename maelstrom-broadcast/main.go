package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Broadcaster struct {
	values_seen []int
	mu sync.Mutex
}

func (b *Broadcaster) SetValue(v int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.values_seen = append(b.values_seen, v)
}

func main() {
	n := maelstrom.NewNode()
	broadcaster := Broadcaster{}
	broadcaster.values_seen = make([]int, 0)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		response := make(map[string]any)
		response["type"] = "broadcast_err"

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if _, ok := body["message"]; !ok {
			return n.Reply(msg, response)
		}

		if v, ok := body["message"].(float64); ok {
			response["type"] = "broadcast_ok"
			broadcaster.SetValue(int(v))
			return n.Reply(msg, response)
		}

		return n.Reply(msg, response)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		body := make(map[string]any)
		body["type"] = "read_ok"

		broadcaster.mu.Lock()
		defer broadcaster.mu.Unlock()
		body["messages"] = broadcaster.values_seen

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		body := make(map[string]any)
		body["type"] = "topology_ok"

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
