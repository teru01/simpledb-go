package dbraft

import (
	"fmt"
	"net"
	"net/rpc"
	"time"
)

type RequestVoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term         uint64
	Success      bool
	LastLogIndex uint64 // 失敗時: Followerのログ末尾Index
}

type RPCHandler interface {
	HandleRequestVote(req *RequestVoteRequest, resp *RequestVoteResponse) error
	HandleAppendEntries(req *AppendEntriesRequest, resp *AppendEntriesResponse) error
}

type Transport interface {
	RequestVote(target string, req *RequestVoteRequest) (*RequestVoteResponse, error)
	AppendEntries(target string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	Start(addr string, handler RPCHandler) error
	Close() error
}

type RaftRPCService struct {
	handler RPCHandler
}

func (s *RaftRPCService) RequestVote(req *RequestVoteRequest, resp *RequestVoteResponse) error {
	return s.handler.HandleRequestVote(req, resp)
}

func (s *RaftRPCService) AppendEntries(req *AppendEntriesRequest, resp *AppendEntriesResponse) error {
	return s.handler.HandleAppendEntries(req, resp)
}

const (
	rpcTimeout       = 1 * time.Second
	rpcServiceName   = "Raft"
	rpcRequestVote   = rpcServiceName + ".RequestVote"
	rpcAppendEntries = rpcServiceName + ".AppendEntries"
)

type NetRPCTransport struct {
	listener net.Listener
}

func NewNetRPCTransport() *NetRPCTransport {
	return &NetRPCTransport{}
}

func (t *NetRPCTransport) Start(addr string, handler RPCHandler) error {
	server := rpc.NewServer()
	if err := server.RegisterName(rpcServiceName, &RaftRPCService{handler: handler}); err != nil {
		return fmt.Errorf("register rpc service: %w", err)
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	t.listener = listener
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go server.ServeConn(conn)
		}
	}()
	return nil
}

func (t *NetRPCTransport) dial(target string) (*rpc.Client, error) {
	conn, err := net.DialTimeout("tcp", target, rpcTimeout)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", target, err)
	}
	return rpc.NewClient(conn), nil
}

func (t *NetRPCTransport) RequestVote(target string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	client, err := t.dial(target)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	resp := &RequestVoteResponse{}
	if err := client.Call(rpcRequestVote, req, resp); err != nil {
		return nil, fmt.Errorf("call RequestVote on %s: %w", target, err)
	}
	return resp, nil
}

func (t *NetRPCTransport) AppendEntries(target string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	client, err := t.dial(target)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	resp := &AppendEntriesResponse{}
	if err := client.Call(rpcAppendEntries, req, resp); err != nil {
		return nil, fmt.Errorf("call AppendEntries on %s: %w", target, err)
	}
	return resp, nil
}

func (t *NetRPCTransport) Close() error {
	if t.listener != nil {
		return t.listener.Close()
	}
	return nil
}
