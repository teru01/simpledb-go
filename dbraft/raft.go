package dbraft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

var ErrNotLeader = errors.New("not leader")

type Role int

const (
	Follower Role = iota
	Candidate
	Leader

	electionTimeout   = 1000 * time.Millisecond
	heartbeatInterval = 100 * time.Millisecond
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

type RaftNode struct {
	mu          sync.Mutex
	id          string
	role        Role
	currentTerm uint64
	votedFor    string
	log         *RaftLog
	commitIndex uint64 // 過半数ノードに複製して確定したログの最後のIndex
	lastApplied uint64 // 実データにapplyされた最新のindex
	leaderID    string

	nextIndex          map[string]uint64 // 次にpeerに送るIndex（楽観的な推測）
	confirmedLastIndex map[string]uint64 // peerが複製済みと確認できた最後のIndex

	peers     []string
	fsm       *FSM
	transport Transport

	resetElectionCh chan struct{}
	applyCh         chan *applyFuture
	stopCh          chan struct{}
}

type applyFuture struct {
	data  []byte
	errCh chan error
}

type Config struct {
	ID        string
	Addr      string
	Peers     []string
	DataDir   string
	FSM       *FSM
	Transport Transport
}

func NewRaftNode(cfg Config) (*RaftNode, error) {
	raftLog, err := NewRaftLog(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("create raft log: %w", err)
	}

	node := &RaftNode{
		id:                 cfg.ID,
		role:               Follower,
		log:                raftLog,
		peers:              cfg.Peers,
		fsm:                cfg.FSM,
		transport:          cfg.Transport,
		resetElectionCh:    make(chan struct{}, 1),
		applyCh:            make(chan *applyFuture, 64),
		stopCh:             make(chan struct{}),
		nextIndex:          make(map[string]uint64),
		confirmedLastIndex: make(map[string]uint64),
	}

	state, err := raftLog.RestoreHardState()
	if err != nil {
		return nil, fmt.Errorf("restore hard state: %w", err)
	}
	node.currentTerm = state.CurrentTerm
	node.votedFor = state.VotedFor

	if err := node.transport.Start(cfg.Addr, node); err != nil {
		return nil, fmt.Errorf("start transport: %w", err)
	}

	go node.run()

	return node, nil
}

func (n *RaftNode) run() {
	for {
		select {
		case <-n.stopCh:
			return
		default:
		}

		switch n.role {
		case Follower:
			n.runFollower()
		case Candidate:
			n.runCandidate()
		case Leader:
			n.runLeader()
		}
	}
}

func (n *RaftNode) runFollower() {
	for {
		timeout := n.randomElectionTimeout()
		select {
		case <-n.stopCh:
			return
		case <-n.resetElectionCh:
		case <-time.After(timeout):
			n.mu.Lock()
			if n.role == Follower {
				slog.Info("election timeout, becoming candidate", "id", n.id, "term", n.currentTerm)
				n.role = Candidate
			}
			n.mu.Unlock()
			return
		}
	}
}

// 候補者として振る舞う
func (n *RaftNode) runCandidate() {
	// 他のgoroutineも入ってるため保持
	n.mu.Lock()
	n.currentTerm++
	n.votedFor = n.id // 自分が投票対象
	currentTerm := n.currentTerm
	lastLogIndex := n.log.LastIndex()
	lastLogTerm := n.log.LastTerm()
	n.persistHardStateLocked()
	n.mu.Unlock()

	slog.Info("starting election", "id", n.id, "term", currentTerm)

	// まず自分に投票
	votes := 1
	voteCh := make(chan bool, len(n.peers))

	for _, peer := range n.peers {
		go func(peer string) {
			// 他全てのピアに投票を呼びかける
			resp, err := n.transport.RequestVote(peer, &RequestVoteRequest{
				Term:         currentTerm,
				CandidateID:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil {
				slog.Debug("RequestVote failed", "peer", peer, "err", err)
				voteCh <- false
				return
			}
			n.mu.Lock()
			// 先を越してるピアがあれば諦める
			if resp.Term > n.currentTerm {
				n.stepDownLocked(resp.Term)
			}
			n.mu.Unlock()
			voteCh <- resp.VoteGranted
		}(peer)
	}

	total := len(n.peers) + 1
	needed := total/2 + 1
	responded := 0
	for {
		select {
		case <-n.stopCh:
			return
		case granted := <-voteCh:
			responded++
			if granted {
				votes++
			}
			if votes >= needed {
				// リーダー昇格
				n.mu.Lock()
				if n.currentTerm == currentTerm && n.role == Candidate {
					slog.Info("won election", "id", n.id, "term", currentTerm, "votes", votes)
					if err := n.becomeLeaderLocked(); err != nil {
						slog.Error("failed to persist no-op entry", "err", err)
						n.role = Follower
					}
				}
				n.mu.Unlock()
				return
			}
			if responded == len(n.peers) {
				n.mu.Lock()
				if n.role == Candidate {
					n.role = Follower
				}
				n.mu.Unlock()
				return
			}
		case <-time.After(n.randomElectionTimeout()):
			return
		}
	}
}

func (n *RaftNode) becomeLeaderLocked() error {
	n.role = Leader
	n.leaderID = n.id
	lastIndex := n.log.LastIndex()
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastIndex + 1 // リーダー昇格時に自分の持ってる最後のログからピアの持っているログの次を推測している
		n.confirmedLastIndex[peer] = 0    // ピアが持っていると確証のもてたログ。最初は確証がない
	}
	return n.log.Append(LogEntry{
		Index: lastIndex + 1,
		Term:  n.currentTerm,
		Data:  nil,
	})
}

// リーダーとして振る舞う
func (n *RaftNode) runLeader() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	n.broadcastAppendEntries()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.Lock()
			if n.role != Leader {
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
			n.broadcastAppendEntries()
		case future := <-n.applyCh:
			// クライアント書き込みのハンドリング
			n.handleApply(future)
		}
	}
}

func (n *RaftNode) handleApply(future *applyFuture) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		future.errCh <- ErrNotLeader
		return
	}
	entry := LogEntry{
		Index: n.log.LastIndex() + 1,
		Term:  n.currentTerm,
		Data:  future.data,
	}
	if err := n.log.Append(entry); err != nil {
		n.mu.Unlock()
		future.errCh <- fmt.Errorf("persist log entry: %w", err)
		return
	}
	n.mu.Unlock()

	n.broadcastAppendEntries()

	n.mu.Lock()
	committed := n.commitIndex >= entry.Index
	n.mu.Unlock()

	if !committed {
		future.errCh <- fmt.Errorf("failed to replicate to majority")
		return
	}

	future.errCh <- n.fsm.Apply(context.Background(), entry.Data, true)

	n.mu.Lock()
	n.lastApplied = entry.Index
	n.mu.Unlock()
}

// 全followerにログを送り、応答を待ってからcommitIndexを更新する
// ハートビート・ログ複製・遅延followerのキャッチアップを兼ねる
func (n *RaftNode) broadcastAppendEntries() {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	commitIndex := n.commitIndex
	n.mu.Unlock()

	var wg sync.WaitGroup
	for _, peer := range n.peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()

			n.mu.Lock()
			nextIdx := n.nextIndex[peer]
			prevLogIndex := nextIdx - 1
			var prevLogTerm uint64
			if e, ok := n.log.GetEntry(prevLogIndex); ok {
				prevLogTerm = e.Term
			}
			entries := n.log.GetRange(nextIdx, n.log.LastIndex())
			n.mu.Unlock()

			resp, err := n.transport.AppendEntries(peer, &AppendEntriesRequest{
				Term:         term,
				LeaderID:     n.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIndex,
			})
			if err != nil {
				slog.Debug("AppendEntries failed", "peer", peer, "err", err)
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if resp.Term > n.currentTerm {
				// より新しいtermを持ったノードがいれば降格する
				n.stepDownLocked(resp.Term)
				return
			}
			if resp.Success {
				if len(entries) > 0 {
					lastEntry := entries[len(entries)-1]
					n.nextIndex[peer] = lastEntry.Index + 1
					n.confirmedLastIndex[peer] = lastEntry.Index
				}
			} else {
				// followerが追いついてないケース。送信するlogの左端を下げて調整する
				n.nextIndex[peer] = resp.LastLogIndex + 1
			}
		}(peer)
	}
	wg.Wait()

	n.advanceCommitIndex()
}

// confirmedLastIndexを見て過半数に複製済みのIndexをcommitIndexに反映する
func (n *RaftNode) advanceCommitIndex() {
	n.mu.Lock()
	oldCommitIndex := n.commitIndex
	for idx := n.commitIndex + 1; idx <= n.log.LastIndex(); idx++ {
		entry, ok := n.log.GetEntry(idx)
		if !ok || entry.Term != n.currentTerm {
			// 前のTermのエントリを直接コミットしてはいけない（現在のタームでコミットできれば連鎖的にコミットされるのでそれを待つ）
			continue
		}
		count := 1
		for _, peer := range n.peers {
			if n.confirmedLastIndex[peer] >= idx {
				count++
			}
		}
		if count > (len(n.peers)+1)/2 {
			n.commitIndex = idx
		}
	}
	n.mu.Unlock()

	// 実際のデータに適応
	for idx := oldCommitIndex + 1; idx <= n.commitIndex; idx++ {
		entry, ok := n.log.GetEntry(idx)
		if !ok {
			break
		}
		if entry.Data == nil {
			n.mu.Lock()
			n.lastApplied = idx
			n.mu.Unlock()
			continue
		}
		if err := n.fsm.Apply(context.Background(), entry.Data, true); err != nil {
			slog.Error("failed to apply entry on leader", "index", idx, "err", err)
			break
		}
		n.mu.Lock()
		n.lastApplied = idx
		n.mu.Unlock()
	}
}

// 投票リクエストに答える
func (n *RaftNode) HandleRequestVote(req *RequestVoteRequest, resp *RequestVoteResponse) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp.Term = n.currentTerm
	resp.VoteGranted = false

	if req.Term < n.currentTerm {
		// 自分より古いタームの候補者には投票しない
		return nil
	}

	if req.Term > n.currentTerm {
		// 自分より新しいタームの候補者がいればfollowerになる（votedForもリセットされる）
		n.stepDownLocked(req.Term)
	}

	if n.votedFor == "" || n.votedFor == req.CandidateID {
		lastLogTerm := n.log.LastTerm()
		lastLogIndex := n.log.LastIndex()
		logOk := req.LastLogTerm > lastLogTerm ||
			(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

		if logOk {
			n.votedFor = req.CandidateID
			n.persistHardStateLocked()
			resp.VoteGranted = true
			n.resetElection()
		}
	}

	resp.Term = n.currentTerm
	return nil
}

// leaderから送られたlogを取り込む
// follower上で呼び出される
func (n *RaftNode) HandleAppendEntries(req *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	n.mu.Lock()
	resp.Term = n.currentTerm
	resp.Success = false

	if req.Term < n.currentTerm {
		// 時代遅れのリーダーから届いたものは門前払い
		n.mu.Unlock()
		return nil
	}

	if req.Term > n.currentTerm {
		n.stepDownLocked(req.Term)
	}

	n.role = Follower
	n.leaderID = req.LeaderID
	n.resetElection()

	if req.PrevLogIndex > 0 {
		entry, ok := n.log.GetEntry(req.PrevLogIndex)
		if !ok || entry.Term != req.PrevLogTerm {
			resp.Term = n.currentTerm
			resp.LastLogIndex = n.log.LastIndex()
			n.mu.Unlock()
			return nil
		}
	}

	for _, entry := range req.Entries {
		existing, ok := n.log.GetEntry(entry.Index)
		if ok && existing.Term != entry.Term {
			if err := n.log.Truncate(entry.Index); err != nil {
				n.mu.Unlock()
				return fmt.Errorf("truncate log at index %d: %w", entry.Index, err)
			}
			if err := n.log.Append(entry); err != nil {
				n.mu.Unlock()
				return fmt.Errorf("append log entry at index %d: %w", entry.Index, err)
			}
		} else if !ok {
			if err := n.log.Append(entry); err != nil {
				n.mu.Unlock()
				return fmt.Errorf("append log entry at index %d: %w", entry.Index, err)
			}
		}
	}

	oldCommitIndex := n.commitIndex
	if req.LeaderCommit > n.commitIndex {
		lastNewIndex := n.log.LastIndex()
		if req.LeaderCommit < lastNewIndex {
			// リーダーからのコミットより自ノードに溜まったログの方が最新
			// 合意確定してないログが溜まってる通常ケース
			// コミット分をリーダーに合わせる
			n.commitIndex = req.LeaderCommit
		} else {
			// 溜まったログよりリーダーからのコミットの方が最新
			// 溜まった分だけコミット（フォロワーは遅れてる）
			n.commitIndex = lastNewIndex
		}
	}
	newCommitIndex := n.commitIndex

	resp.Term = n.currentTerm
	resp.Success = true
	n.mu.Unlock()

	// 実際のデータに適応
	for idx := oldCommitIndex + 1; idx <= newCommitIndex; idx++ {
		entry, ok := n.log.GetEntry(idx)
		if !ok {
			break
		}
		if entry.Data == nil {
			n.mu.Lock()
			n.lastApplied = idx
			n.mu.Unlock()
			continue
		}
		if err := n.fsm.Apply(context.Background(), entry.Data, false); err != nil {
			slog.Error("failed to apply entry on follower", "index", idx, "err", err)
			break
		}
		n.mu.Lock()
		n.lastApplied = idx
		n.mu.Unlock()
	}

	return nil
}

// dataを書き込む
func (n *RaftNode) Apply(data []byte) error {
	future := &applyFuture{
		data:  data,
		errCh: make(chan error, 1),
	}

	select {
	case n.applyCh <- future:
	case <-time.After(10 * time.Second):
		return fmt.Errorf("apply timeout: channel full")
	}

	return <-future.errCh
}

func (n *RaftNode) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader
}

func (n *RaftNode) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

func (n *RaftNode) State() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

func (n *RaftNode) Stop() {
	close(n.stopCh)
	n.transport.Close()
	n.log.Persist()
}

// フォロワーに降りてtermを更新。投票先をリセットして永続化
func (n *RaftNode) stepDownLocked(term uint64) {
	n.currentTerm = term
	n.role = Follower
	n.votedFor = ""
	n.persistHardStateLocked()
}

func (n *RaftNode) persistHardStateLocked() {
	state := HardState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
	}
	if err := n.log.PersistHardState(state); err != nil {
		slog.Error("failed to persist hard state", "err", err)
	}
}

func (n *RaftNode) resetElection() {
	select {
	case n.resetElectionCh <- struct{}{}:
	default:
	}
}

// 選挙時のランダムなタイムアウト、これによって他のノードとタイミングをずらして成功率を高める
func (n *RaftNode) randomElectionTimeout() time.Duration {
	return electionTimeout + time.Duration(rand.Int64N(int64(electionTimeout)))
}
