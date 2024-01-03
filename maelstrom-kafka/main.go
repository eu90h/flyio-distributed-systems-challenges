package main

import (
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
	data map[string][]LogEntry
	committed_offsets map[string]int
	//maps keys to the next file offset
	next_offset map[string]int
	neighbors []string
	node *maelstrom.Node
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
	msg := SendMessage{}
	if err := json.Unmarshal(raw_msg.Body, &msg); err != nil {
		log.Fatal(err)
		return err
	}
	if msg.Type != "send" {
		return errors.New("HandleSendMessage tried to process incorrect message type!")
	}
	ak.mu.Lock()
	if _, ok := ak.next_offset[msg.Key]; !ok {
		ak.next_offset[msg.Key] = 0
	}
	ak.next_offset[msg.Key] += 1
	le := LogEntry{ak.next_offset[msg.Key], msg.Message}
	ak.mu.Unlock()
	offset, err := ak.WriteLogEntry(msg.Key, le)
	if err != nil {
		log.Println(err)
		return err
	}
	response := SendMessageResponse{"send_ok", offset}
	return ak.node.Reply(raw_msg, response)
}

func (ak *Akfak) HandlePoll(raw_msg maelstrom.Message) error {
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
		ak.mu.Lock()
		if _, ok := ak.next_offset[key]; ok {
			Off := ak.next_offset[key]
			log.Printf("ak.next_offset[%v] = %v\n", key, ak.next_offset[key])
			ak.mu.Unlock()
			response.Messages[key] = make([][2]int, 0)
			for offset := 1; offset <= Off; offset += 1 {
				if offset >= msg.Offsets[key] {
					le, err := ak.ReadLogEntry(key, offset)
					if err == nil {
						log.Printf("read value %v @ offset %v for key %v\n", le.Value, offset, key)
						response.Messages[key] = append(response.Messages[key], [2]int{offset, int(le.Value)})
					} else {
						log.Println(err)
						log.Printf("failed to read offset %v for key %v\n", offset, key)
					}
				}
			}
		} else {
			ak.mu.Unlock()
		}
	}
	return ak.node.Reply(raw_msg, response)
}

func (ak *Akfak) HandleCommitOffsets(raw_msg maelstrom.Message) error {
	msg := CommitOffsetsMessage{}
	if err := json.Unmarshal(raw_msg.Body, &msg); err != nil {
		log.Fatal(err)
		return err
	}
	if msg.Type != "commit_offsets" {
		return errors.New("HandleCommitOffsets tried to process incorrect message type!")
	}
	ak.mu.Lock()
	for key := range msg.Offsets {
		ak.committed_offsets[key] = msg.Offsets[key]
	}
	ak.mu.Unlock()
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
			log.Printf("node %v checking comitted offsets for %v: %v\n", ak.node.ID(), key, ak.committed_offsets[key])
			response.Offsets[key] = ak.committed_offsets[key]
		} else {
			log.Printf("node %v checking comitted offsets for %v: []\n", ak.node.ID(), key)
		}
	}
	ak.mu.Unlock()
	return ak.node.Reply(raw_msg, response)
}

func (ak *Akfak) WriteLogEntry(key string, le LogEntry) (int, error) {
	ak.mu.Lock()
	defer ak.mu.Unlock()
	if _, ok := ak.data[key]; ok {
		ak.data[key] = append(ak.data[key], le)
	} else {
		ak.data[key] = make([]LogEntry, 0)
		ak.data[key] = append(ak.data[key], le)
	}
	return le.Offset, nil
}

func (ak *Akfak) ReadLogEntry(key string, offset int) (LogEntry, error) {
	ak.mu.Lock()
	defer ak.mu.Unlock()
	for _, le := range ak.data[key] {
		if le.Offset == offset {
			return le, nil
		}
	}
	return LogEntry{}, errors.New("LogEntry not found")
}

func main() {
	ak := Akfak{}
	ak.node = maelstrom.NewNode()
	ak.neighbors = make([]string, 8)
	ak.committed_offsets = make(map[string]int)
	ak.next_offset = make(map[string]int)
	ak.data = make(map[string][]LogEntry)

	ak.node.Handle("init", ak.HandleInit)
	ak.node.Handle("send", ak.HandleSend)
	ak.node.Handle("poll", ak.HandlePoll)
	ak.node.Handle("commit_offsets", ak.HandleCommitOffsets)
	ak.node.Handle("list_committed_offsets", ak.HandleListCommittedOffsets)

	if err := ak.node.Run(); err != nil {
		log.Fatal(err)
	}
}
