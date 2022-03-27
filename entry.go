package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
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
	// 当前节点的其他节点, key 表示节点的唯一值, value 表示当前节点的信息
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
				rf.voteCount = 1
				// 向其他的节点广播信息
				go rf.broadcastRequestVote()
				select {
				// 超时从新变回Follower
				case <-time.After(time.Duration(rand.Intn(5000-300)+300) * time.Millisecond):
					rf.state = Follower
				// 选举成功
				case <-rf.toLeaderC:
					fmt.Println("node:%d I am Leader\n", rf.me)
					rf.state = Leader
				}
			case Leader:
				// 每隔100ms向其他的节点发起一次心跳hearbeat
				rf.broadcastHeartbeat()
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
}

// Candidate 节点广播投票申请
func (rf *Raft) broadcastRequestVote() {
	// 设置request
	var args = VoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me, // 当前节点的唯一id
	}
	// rs.nodes 保存了其他node 节点的信息, 遍历node节点, 发送投票信息
	for i := range rf.nodes {
		go func(i int) {
			var reply VoteReply
			// 发送投票信息
			rf.sendRequestVote(i, args, &reply)
		}(i)
	}
}

// 发送申请到某个节点
// serverId - server 唯一标识
// args - request  内容
// reply - Follower response
func (rf *Raft) sendRequestVote(serverId int, args VoteArgs, reply *VoteReply) {
	// 创建client
	client, err := rpc.DialHTTP("tcp", rf.nodes[serverId].address)
	if err != nil {
		log.Fatalf("dialing: ", err)
	}
	defer client.Close()
	// 走rpc调用Follower 节点的 requestVote 方法
	_ = client.Call("Raft.RequestVote", args, reply)

	// 如果candidate节点Term小于follower节点
	// 当前candidate节点无效, candidate节点变为follower节点
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		// 当前节点从candidate变为Follower节点
		rf.state = Follower
		rf.votedFor = -1
		return
	}
	// 成功获取到Follower的投票
	if reply.VoteGranted {
		// 票数加1
		rf.voteCount++
		// 判断获取的投票数是否大于集群的一半
		if rf.voteCount > len(rf.nodes)/2+1 {
			// 发送通知标识正真的成为了Leader
			rf.toLeaderC <- true
		}
	}
}

// 处理完Candidate节点消息后,为Follower节点处理投票申请
// follower 节点处理投票请求rpc请求
func (rf *Raft) RequestVote(args VoteArgs, reply *VoteReply) error {
	// 如果candidate节点的term小于自己的Term, 那么拒绝投票
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return nil
	}
	// follower节点未给其他的节点投票, 投票成功
	if rf.votedFor == -1 {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID // 表示投票给了谁
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return nil
	}
	// 其他的情况
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return nil
}

// Leader 节点发送心跳给其他的节点, 保持自己的Leader地位
func (rf *Raft) broadcastHeartbeat() {

}

// 投票选举
// 使用golang 自带的rpc进行通讯
// request  rpc
type VoteArgs struct {
	// 当前任期
	Term int
	// 候选人Id
	CandidateID int
}

// response RPC
type VoteReply struct {
	// 当前任期号
	// 如果其他节点的Term大于当前Candidate节点的Term, 以便Candidate去更新自己的任期号
	Term int
	// 候选人赢得此张选票时为真
	VoteGranted bool
}
