package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type AddMessage struct {
	Type string
	Delta int
}

type ReadMessage struct {
	Type string
}

type GCounter struct {
	node *maelstrom.Node
	kv *maelstrom.KV
	mu sync.Mutex
}

const COUNTER_KEY = "counter"

func (gc *GCounter) HandleAddMessage(raw_msg maelstrom.Message) error {
	msg := AddMessage{}
	if err := json.Unmarshal(raw_msg.Body, &msg); err != nil {
		return err
	}
	if msg.Type != "add" {
		return errors.New("HandleAddMessage tried to process non-add message!")
	}

	ctx := context.Background()

	gc.mu.Lock()
	defer gc.mu.Unlock()
	r, err := gc.kv.Read(ctx, COUNTER_KEY)
	v := 0
	if err != nil {
		var rpcError *maelstrom.RPCError
		if errors.As(err, &rpcError) {
			if rpcError.Code == maelstrom.KeyDoesNotExist {
				gc.kv.Write(ctx, COUNTER_KEY, 0)
				v = 0
			} else {
				return err
			}
		} else {
			return err
		}
	} else {
		v = r.(int)
	}
	log.Printf("%v + %d = %v\n", v, msg.Delta, v + msg.Delta)
	err = gc.kv.Write(ctx, COUNTER_KEY, v + msg.Delta)
	if err != nil {
		return err
	}
	
	response := make(map[string]any)
	response["type"] = "add_ok"
	return gc.node.Reply(raw_msg, response)
}

func (gc *GCounter) HandleNodeInit(raw_msg maelstrom.Message) error {
	ctx := context.Background()
	gc.mu.Lock()
	defer gc.mu.Unlock()
	_, err := gc.kv.Read(ctx, COUNTER_KEY)
	if err == nil {
		response := make(map[string]any)
		response["type"] = "init_ok"
		return gc.node.Reply(raw_msg, response)
	}
	var rpcError *maelstrom.RPCError
	if errors.As(err, &rpcError) {
		if rpcError.Code == maelstrom.KeyDoesNotExist {
			gc.kv.Write(ctx, COUNTER_KEY, 0)
		} else {
			return err
		}
	}
	response := make(map[string]any)
	response["type"] = "init_ok"
	return gc.node.Reply(raw_msg, response)
}

func (gc *GCounter) HandleReadMessage(raw_msg maelstrom.Message) error {
	msg := ReadMessage{}
	if err := json.Unmarshal(raw_msg.Body, &msg); err != nil {
		return err
	}
	if msg.Type != "read" {
		return errors.New("HandleReadMessage tried to process non-read message!")
	}
	ctx := context.Background()
	gc.mu.Lock()
	defer gc.mu.Unlock()
	v, err := gc.kv.Read(ctx, COUNTER_KEY)
	if err != nil {
		var rpcError *maelstrom.RPCError
		if errors.As(err, &rpcError) {
			if rpcError.Code == maelstrom.KeyDoesNotExist {
				gc.kv.Write(ctx, COUNTER_KEY, 0)
				v = 0
			} else {
				return err
			}
		} else {
			return err
		}
	}
	response := make(map[string]any)
	response["type"] = "read_ok"
	response["value"] = v.(int)
	return gc.node.Reply(raw_msg, response)
}

func main() {
	n := maelstrom.NewNode()
	gc := GCounter{}
	gc.node = n
	gc.kv = maelstrom.NewSeqKV(n)

	n.Handle("init", gc.HandleNodeInit)
	n.Handle("add", gc.HandleAddMessage)
	n.Handle("read", gc.HandleReadMessage)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
