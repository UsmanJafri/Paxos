package paxos

import (
	"Paxos/rpc/paxosrpc"
	"errors"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

var PROPOSE_TIMEOUT = 15 * time.Second

type paxosNode struct {
	nodes         map[int]*rpc.Client
	put           chan putRequest
	get           chan getRequest
	getPaxosState chan getPaxosStateRequest
}

type putRequest struct {
	key   string
	value interface{}
}

type getResponse struct {
	value interface{}
	ok    bool
}

type getRequest struct {
	key      string
	response chan getResponse
}

type getPaxosStateRequest struct {
	key      string
	response chan getPaxosStateResponse
}

type getPaxosStateResponse struct {
	state paxosState
	ok    bool
}

type paxosState struct {
	minProposal      int
	acceptedProposal int
	acceptedValue    interface{}
}

// Desc:
// NewPaxosNode creates a new PaxosNode. This function should return only when
// all nodes have joined the ring, and should return a non-nil error if this node
// could not be started in spite of dialing any other nodes numRetries times.
//
// Params:
// myHostPort: the hostport string of this new node. We use tcp in this project.
//			   	Note: Please listen to this port rather than hostMap[srvId]
// hostMap: a map from all node IDs to their hostports.
//				Note: Please connect to hostMap[srvId] rather than myHostPort
//				when this node try to make rpc call to itself.
// numNodes: the number of nodes in the ring
// numRetries: if we can't connect with some nodes in hostMap after numRetries attempts, an error should be returned
// replace: a flag which indicates whether this node is a replacement for a node which failed.
func NewPaxosNode(myHostPort string, hostMap map[int]string, numNodes, srvId, numRetries int, replace bool) (PaxosNode, error) {
	myNode := new(paxosNode)
	myNode.nodes = make(map[int]*rpc.Client)
	myNode.put = make(chan putRequest)
	myNode.get = make(chan getRequest)
	myNode.getPaxosState = make(chan getPaxosStateRequest)

	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}
	rpcServer := rpc.NewServer()
	rpcServer.Register(paxosrpc.Wrap(myNode))
	rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	go http.Serve(listener, nil)

	for nodeID, node := range hostMap {
		for try := 0; try < numRetries; try++ {
			client, err := rpc.DialHTTP("tcp", node)
			if err == nil {
				myNode.nodes[nodeID] = client
				break
			} else if try == numRetries-1 {
				return nil, err
			}
			time.Sleep(time.Duration(1) * time.Second)
		}
	}
	go myNode.storeHandler()
	go myNode.paxosStateHandler()
	return myNode, nil
}

// Desc:
// GetNextProposalNumber generates a proposal number which will be passed to
// Propose. Proposal numbers should not repeat for a key, and for a particular
// <node, key> pair, they should be strictly increasing.
//
// Params:
// args: the key to propose
// reply: the next proposal number for the given key
func (pn *paxosNode) GetNextProposalNumber(args *paxosrpc.ProposalNumberArgs, reply *paxosrpc.ProposalNumberReply) error {
	responseChan := make(chan getPaxosStateResponse)
	pn.getPaxosState <- getPaxosStateRequest{args.Key, responseChan}
	response := <-responseChan
	if !response.ok {
		reply.N = 1
	} else {
		reply.N = response.state.minProposal + 1
	}
	return nil
}

// Desc:
// Propose initializes proposing a value for a key, and replies with the
// value that was committed for that key. Propose should not return until
// a value has been committed, or PROPOSE_TIMEOUT seconds have passed.
//
// Params:
// args: the key, value pair to propose together with the proposal number returned by GetNextProposalNumber
// reply: value that was actually committed for the given key
func (pn *paxosNode) Propose(args *paxosrpc.ProposeArgs, reply *paxosrpc.ProposeReply) error {
	return errors.New("not implemented")
}

// Desc:
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
func (pn *paxosNode) GetValue(args *paxosrpc.GetValueArgs, reply *paxosrpc.GetValueReply) error {
	return errors.New("not implemented")
}

// Desc:
// Receive a Prepare message from another Paxos Node. The message contains
// the key whose value is being proposed by the node sending the prepare
// message. This function should respond with Status OK if the prepare is
// accepted and Reject otherwise.
//
// Params:
// args: the Prepare Message, you must include RequesterId when you call this API
// reply: the Prepare Reply Message
func (pn *paxosNode) RecvPrepare(args *paxosrpc.PrepareArgs, reply *paxosrpc.PrepareReply) error {
	return errors.New("not implemented")
}

// Desc:
// Receive an Accept message from another Paxos Node. The message contains
// the key whose value is being proposed by the node sending the accept
// message. This function should respond with Status OK if the prepare is
// accepted and Reject otherwise.
//
// Params:
// args: the Please Accept Message, you must include RequesterId when you call this API
// reply: the Accept Reply Message
func (pn *paxosNode) RecvAccept(args *paxosrpc.AcceptArgs, reply *paxosrpc.AcceptReply) error {
	return errors.New("not implemented")
}

// Desc:
// Receive a Commit message from another Paxos Node. The message contains
// the key whose value was proposed by the node sending the commit
// message.
//
// Params:
// args: the Commit Message, you must include RequesterId when you call this API
// reply: the Commit Reply Message
func (pn *paxosNode) RecvCommit(args *paxosrpc.CommitArgs, reply *paxosrpc.CommitReply) error {
	return errors.New("not implemented")
}

// Desc:
// Notify another node of a replacement server which has started up. The
// message contains the Server ID of the node being replaced, and the
// hostport of the replacement node
//
// Params:
// args: the id and the hostport of the server being replaced
// reply: no use
func (pn *paxosNode) RecvReplaceServer(args *paxosrpc.ReplaceServerArgs, reply *paxosrpc.ReplaceServerReply) error {
	return errors.New("not implemented")
}

// Desc:
// Request the value that was agreed upon for a particular round. A node
// receiving this message should reply with the data (as an array of bytes)
// needed to make the replacement server aware of the keys and values
// committed so far.
//
// Params:
// args: no use
// reply: a byte array containing necessary data used by replacement server to recover
func (pn *paxosNode) RecvReplaceCatchup(args *paxosrpc.ReplaceCatchupArgs, reply *paxosrpc.ReplaceCatchupReply) error {
	return errors.New("not implemented")
}

func (pn *paxosNode) storeHandler() {
	store := make(map[string]interface{})

	for {
		select {
		case put := <-pn.put:
			store[put.key] = put.value
		case get := <-pn.get:
			value, ok := store[get.key]
			get.response <- getResponse{value, ok}
		}
	}
}

func (pn *paxosNode) paxosStateHandler() {
	paxosStore := make(map[string]paxosState)
	for {
		select {
		case get := <-pn.getPaxosState:
			value, ok := paxosStore[get.key]
			get.response <- getPaxosStateResponse{value, ok}
		}
	}
}
