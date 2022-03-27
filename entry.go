package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

// 节点定义,存储每个节点的信息
type node struct {
	connect bool   // 节点状态
	address string // 节点地址
}

// 1. RaftNode 节点, 用于标识每一个节点的Raft状态
type State int

// Raft节点的三种状态, Follower, Leader, Candidate
const (
	Follower State = iota + 1
	Candidate
	Leader
)

// raft Node
type Raft struct {
	// 当前节点id
	me int
	// 当前节点的其他节点
	nodes map[int]*node
	// 当前节点的状态
	state State
	// 当前任期
	currentTerm int
	// 当前任期投票给了谁
	votedFor int
	// 当前任期获取的投票数量
	voteCount int
	// hearbeat channel
	heartbeatc chan bool
	// to leader channel
	toLeaderC chan bool
}

// 创建新的节点
func newNode(address string) *node {
	node := &node{}
	node.address = address
	return node
}

//创建raft
func (rf *Raft) start() {
	// 初始化raft节点
	rf.state = Follower
	rf.currentTerm = 0
	rf.currentTerm = -1
	rf.heartbeatc = make(chan bool)
	rf.toLeaderC = make(chan bool)
	// 节点状态变更以及RPC处理
	//...
	go func() {
		rand.Seed(time.Now().UnixNano())
		// 不断处理节点行为和RPC
		for {
			switch rf.state {
			// 如果是Follower节点的话
			case Follower:
				select {
				// 从 headerbeatc管道成功接受到hearbeat
				case <-rf.heartbeatc:
					log.Printf("follower-%d recived hearbeat\n", rf.me)
					// 超时变成候选者
				case <-time.After(time.Duration(rand.Intn(500-300)+300) * time.Millisecond):
					log.Printf("follower-%d timeout, become candidate\n", rf.me)
					rf.state = Candidate
				}
			// 其他状态的处理..
			case Candidate:
				fmt.Println("Node: %d, I am candidate now\n", rf.me)
				// 当前Term +1
				rf.currentTerm++
				// 为自己投票
				rf.votedFor = rf.me
			}
		}
	}()
}
