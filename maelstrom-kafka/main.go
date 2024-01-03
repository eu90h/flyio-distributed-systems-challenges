package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// This message requests that a "msg" value be append to a log identified by "key"
type SendMessage struct {
	Type string `json:"type"`
	Key string `json:"key"`
	Message int `json:"msg"`
}

type SendMessageResponse struct {
	Type string `json:"type"`
	Offset int `json:"offset"`
}

type PollMessage struct {
	Type string `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type PollMessageResponse struct {
	Type string `json:"type"`
	Messages map[string][][2]int `json:"msgs"`
}

type CommitOffsetsMessage struct {
	Type string `json:"type"`
	Offsets map[string]int `json:"offsets"`
}
/*
type GossipCommittedOffsetsMessage struct {
	Type string `json:"type"`
	CommittedOffsets map[string]int `json:"committed_offsets"`
}

type GossipNextOffsetMessage struct {
	Type string `json:"type"`
	NextOffset map[string]int `json:"next_offsets"`
}*/

type CommitOffsetsMessageResponse struct {
	Type string `json:"type"`
}

type ListCommittedOffsets struct {
	Type string `json:"type"`
	Keys []string `json:"keys"`
}

type ListCommittedOffsetsResponse struct {
	Type string `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type LogEntry struct {
	Offset int `json:"offset"`
	Value int `json:"value"`
}

type InitMessage struct {
	Type string `json:"type"`
	NodeID string `json:"node_id"`
	NodeIDs []string `json:"node_ids"`
	MsgID int `json:"msg_id"`
}

type Akfak struct {
	neighbors []string
	node *maelstrom.Node
	kv *maelstrom.KV
	mu sync.Mutex
}

func (ak *Akfak) HandleInit(raw_msg maelstrom.Message) error {
	init_msg := InitMessage{}
	if err := json.Unmarshal(raw_msg.Body, &init_msg); err != nil {
		log.Fatal(err)
		return err
	}
	if init_msg.Type != "init" {
		log.Fatal("wrong type")
		return errors.New("HandleInit tried to process incorrect message type!")
	}
	ak.mu.Lock()
	ak.neighbors = init_msg.NodeIDs
	log.Println(ak.neighbors)
	ak.mu.Unlock()
	return nil
}

func (ak *Akfak) GetNextOffset(key string) (int, error) {
	ctx := context.Background()
	next_offset_key := fmt.Sprintf("next_offset_%s", key)	
	next_offset, err := ak.kv.ReadInt(ctx, next_offset_key)
	if err != nil {
		target := &maelstrom.RPCError{}
		if errors.As(err, &target) {
			if target.Code == maelstrom.KeyDoesNotExist {
				next_offset = 1
				err = ak.kv.Write(ctx, next_offset_key, 2)
				if err != nil {
					return 0, err
				}
				return next_offset, nil
			} else {
				return 0, err
			}
		} else {
			return 0, err
		}
	}
	candidate := next_offset
	for ;; {
		err = ak.kv.CompareAndSwap(ctx, next_offset_key, candidate, candidate + 1, true)
		if err == nil {
			return candidate , nil
		}
		candidate += 1
	}
}

func (ak *Akfak) HandleSend(raw_msg maelstrom.Message) error {
	ak.mu.Lock()
	defer ak.mu.Unlock()
	msg := SendMessage{}
	if err := json.Unmarshal(raw_msg.Body, &msg); err != nil {
		log.Fatal(err)
		return err
	}
	if msg.Type != "send" {
		return errors.New("HandleSendMessage tried to process incorrect message type!")
	}
	candidate, err := ak.GetNextOffset(msg.Key)
	if err != nil {
		log.Println(err)
		return err
	}
	log.Printf("key: %s\t\toffset: %d\n", msg.Key, candidate)
	le := LogEntry{candidate, msg.Message}
	err = ak.WriteLogEntry(msg.Key, le)
	if err != nil {
		log.Println(err)
		return err
	}
	response := SendMessageResponse{"send_ok", le.Offset}
	return ak.node.Reply(raw_msg, response)
}

func (ak *Akfak) HandlePoll(raw_msg maelstrom.Message) error {
	ak.mu.Lock()
	defer ak.mu.Unlock()
	msg := PollMessage{}
	if err := json.Unmarshal(raw_msg.Body, &msg); err != nil {
		log.Fatal(err)
		return err
	}
	if msg.Type != "poll" {
		return errors.New("HandlePollMessage tried to process incorrect message type!")
	}
	response := PollMessageResponse{"poll_ok", make(map[string][][2]int)}
	for key := range msg.Offsets {
		ctx := context.Background()
		next_offset_key := fmt.Sprintf("next_offset_%s", key)	
		next_offset, err := ak.kv.ReadInt(ctx, next_offset_key)
		if err != nil {
			return err
		}
		response.Messages[key] = make([][2]int, 0)
		for offset := msg.Offsets[key]; offset <= next_offset + 100; offset += 1 {
			le, err := ak.ReadLogEntry(key, offset)
			if err == nil {
				response.Messages[key] = append(response.Messages[key], [2]int{le.Offset, int(le.Value)})
			}
		}
	}
	return ak.node.Reply(raw_msg, response)
}

func (ak *Akfak) HandleCommitOffsets(raw_msg maelstrom.Message) error {
	ak.mu.Lock()
	defer ak.mu.Unlock()
	msg := CommitOffsetsMessage{}
	if err := json.Unmarshal(raw_msg.Body, &msg); err != nil {
		log.Fatal(err)
		return err
	}
	if msg.Type != "commit_offsets" {
		return errors.New("HandleCommitOffsets tried to process incorrect message type!")
	}
	for key := range msg.Offsets {
		ctx := context.Background()
		committed_offsets_key := fmt.Sprintf("committed_offsets_key:%s", key)
		err := ak.kv.Write(ctx, committed_offsets_key, msg.Offsets[key])
		if err != nil {
			return err
		}
	}
	response := CommitOffsetsMessageResponse{"commit_offsets_ok"}
	return ak.node.Reply(raw_msg, response)
}

func (ak *Akfak) HandleListCommittedOffsets(raw_msg maelstrom.Message) error {
	msg := ListCommittedOffsets{}
	if err := json.Unmarshal(raw_msg.Body, &msg); err != nil {
		log.Fatal(err)
		return err
	}
	if msg.Type != "list_committed_offsets" {
		return errors.New("HandleListCommittedOffsets tried to process incorrect message type!")
	}
	response := ListCommittedOffsetsResponse{"list_committed_offsets_ok", nil}
	response.Offsets = make(map[string]int)
	ak.mu.Lock()
	for _, key := range msg.Keys {
		ctx := context.Background()
		committed_offsets_key := fmt.Sprintf("committed_offsets_key:%s", key)
		offset, err := ak.kv.ReadInt(ctx, committed_offsets_key)
		if err != nil {
			target := &maelstrom.RPCError{}
			if !errors.As(err, &target) {
				return err
			}
		} else {
			response.Offsets[key] = offset
		}
	}
	ak.mu.Unlock()
	return ak.node.Reply(raw_msg, response)
}

func (ak *Akfak) WriteLogEntry(key string, le LogEntry) error {
	ctx := context.Background()
	entry_key := fmt.Sprintf("entry_key:%s_offset:%d", key, le.Offset)
	ctx = context.Background()
	err := ak.kv.Write(ctx, entry_key, le.Value)
	if err != nil {
		log.Printf("[WriteLogEntry] key: %s, le: %v, err: %v\n", key, le, err)
		return err
	}
	return nil
}

func (ak *Akfak) ReadLogEntry(key string, offset int) (LogEntry, error) {
	ctx := context.Background()
	entry_key := fmt.Sprintf("entry_key:%s_offset:%d", key, offset)
	value, err := ak.kv.ReadInt(ctx, entry_key)
	if err != nil {
		log.Printf("[ReadLogEntry] key: %s, offset: %v, err: %v\n", key, offset, err)
		return LogEntry{}, err
	}
	return LogEntry{offset, value}, nil
}

func main() {
	ak := Akfak{}
	ak.node = maelstrom.NewNode()
	ak.neighbors = make([]string, 8)
	ak.kv = maelstrom.NewLinKV(ak.node)

	ak.node.Handle("init", ak.HandleInit)
	ak.node.Handle("send", ak.HandleSend)
	ak.node.Handle("poll", ak.HandlePoll)
	ak.node.Handle("commit_offsets", ak.HandleCommitOffsets)
	ak.node.Handle("list_committed_offsets", ak.HandleListCommittedOffsets)

	if err := ak.node.Run(); err != nil {
		log.Fatal(err)
	}
}
