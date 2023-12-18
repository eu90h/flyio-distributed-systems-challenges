package main

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "generate_ok"
		
		
		b := make([]byte, 8)
		_, err := rand.Read(b)
		if err != nil {
			return err
		}

		body["id"] = binary.BigEndian.Uint64(b)

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
