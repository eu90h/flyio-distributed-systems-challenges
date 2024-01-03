package main

import (
	"context"
	"encoding/json"
	"errors"
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

type GossipCommittedOffsetsMessage struct {
	Type string `json:"type"`
	CommittedOffsets map[string]int `json:"committed_offsets"`
}

type GossipNextOffsetMessage struct {
	Type string `json:"type"`
	NextOffset map[string]int `json:"next_offsets"`
}

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
	committed_offsets map[string]int
	//maps keys to the next file offset
	next_offset map[string]int
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
	if _, ok := ak.next_offset[msg.Key]; !ok {
		ak.next_offset[msg.Key] = 0
	}
	ak.next_offset[msg.Key] += 1
	le := LogEntry{ak.next_offset[msg.Key], msg.Message}
	offset, err := ak.WriteLogEntry(msg.Key, le)
	if err != nil {
		log.Println(err)
		return err
	}
	gossip := GossipNextOffsetMessage{}
	gossip.Type = "gossip_next_offset"
	gossip.NextOffset = make(map[string]int)
	gossip.NextOffset[msg.Key] = ak.next_offset[msg.Key]
	ak.Multicast(gossip)
	response := SendMessageResponse{"send_ok", offset}
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
		if _, ok := ak.next_offset[key]; ok {
			Off := ak.next_offset[key]
			response.Messages[key] = make([][2]int, 0)
			for offset := 1; offset <= Off; offset += 1 {
				if offset >= msg.Offsets[key] {
					le, err := ak.ReadLogEntry(key, offset)
					if err == nil {
						response.Messages[key] = append(response.Messages[key], [2]int{offset, int(le.Value)})
					} else {
						log.Println(err)
					}
				}
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
		ak.committed_offsets[key] = msg.Offsets[key]
	}
	gossip := GossipCommittedOffsetsMessage{}
	gossip.Type = "gossip_committed_offsets"
	gossip.CommittedOffsets = msg.Offsets
	ak.Multicast(gossip)
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
		if _, ok := ak.committed_offsets[key]; ok { 
			response.Offsets[key] = ak.committed_offsets[key]
		}
	}
	ak.mu.Unlock()
	return ak.node.Reply(raw_msg, response)
}

func (ak *Akfak) WriteLogEntry(key string, le LogEntry) (int, error) {
	ctx := context.Background()
	entries := make([]LogEntry, 128)
	err := ak.kv.ReadInto(ctx, key, &entries)
	if err != nil {
		target := &maelstrom.RPCError{}
		if errors.As(err, &target) {
			if target.Code == maelstrom.KeyDoesNotExist {
				ctx2 := context.Background()
				err = ak.kv.Write(ctx2, key, []LogEntry{le})
				if err != nil {
					return 0, err
				}
				return le.Offset, nil
			} else {
				return 0, err
			}
		} else {
			return 0, err
		}
	}
	ctx = context.Background()
	err = ak.kv.Write(ctx, key, append(entries, le))
	if err != nil {
		return 0, err
	}
	return le.Offset, nil
}

func (ak *Akfak) ReadLogEntry(key string, offset int) (LogEntry, error) {
	ctx := context.Background()
	entries := make([]LogEntry, 128)
	err := ak.kv.ReadInto(ctx, key, &entries)
	if err != nil {
		return LogEntry{}, err
	}
	for _, le := range entries {
		if le.Offset == offset {
			return le, nil
		}
	}
	return LogEntry{}, errors.New("LogEntry not found")
}

func (ak *Akfak) HandleGossipNextOffset(msg maelstrom.Message) error {
	gossip := GossipNextOffsetMessage{}
	if err := json.Unmarshal(msg.Body, &gossip); err != nil {
		return err
	}
	ak.mu.Lock()
	for key := range gossip.NextOffset {
		if ak.next_offset[key] < gossip.NextOffset[key] {
			ak.next_offset[key] = gossip.NextOffset[key]
		}
	}
	ak.mu.Unlock()
	reply := make(map[string]any)
	reply["type"] = "gossip_next_offset_ok"
	return ak.node.Reply(msg, reply)
}

func (ak *Akfak) HandleGossipCommittedOffsets(msg maelstrom.Message) error {
	gossip := GossipCommittedOffsetsMessage{}
	if err := json.Unmarshal(msg.Body, &gossip); err != nil {
		return err
	}
	ak.mu.Lock()
	for key := range gossip.CommittedOffsets {
		ak.committed_offsets[key] = gossip.CommittedOffsets[key]
	}
	ak.mu.Unlock()
	reply := make(map[string]any)
	reply["type"] = "gossip_committed_offsets_ok"
	return ak.node.Reply(msg, reply)
}

func (ak *Akfak) HandleGossipResponse(msg maelstrom.Message) error {
	return nil
}

func (ak *Akfak) Multicast(msg interface{}) {
	if ak.node == nil {
		panic("me is nil")
	}
	for _, node := range ak.neighbors {
		if node != ak.node.ID() {
			err := ak.node.RPC(node, msg, ak.HandleGossipResponse)
			if err != nil {
				log.Printf("problem sending message to node %v\n", node)
			}
		}
	}
}

func main() {
	ak := Akfak{}
	ak.node = maelstrom.NewNode()
	ak.neighbors = make([]string, 8)
	ak.committed_offsets = make(map[string]int)
	ak.next_offset = make(map[string]int)
	ak.kv = maelstrom.NewLinKV(ak.node)

	ak.node.Handle("init", ak.HandleInit)
	ak.node.Handle("send", ak.HandleSend)
	ak.node.Handle("poll", ak.HandlePoll)
	ak.node.Handle("commit_offsets", ak.HandleCommitOffsets)
	ak.node.Handle("list_committed_offsets", ak.HandleListCommittedOffsets)
	ak.node.Handle("gossip_committed_offsets", ak.HandleGossipCommittedOffsets)
	ak.node.Handle("gossip_next_offset", ak.HandleGossipNextOffset)

	if err := ak.node.Run(); err != nil {
		log.Fatal(err)
	}
}
