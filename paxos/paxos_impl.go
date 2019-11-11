package paxos

import (
	"Paxos/rpc/paxosrpc"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

var PROPOSE_TIMEOUT = 15 * time.Second

type paxosNode struct {
	id            int
	nodes         map[int]*rpc.Client
	put           chan putRequest
	get           chan getRequest
	getPaxosState chan getPaxosStateRequest
	putPaxosState chan putPaxosStateRequest
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

type putPaxosStateRequest struct {
	key   string
	value paxosState
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
	myNode.id = srvId
	myNode.nodes = make(map[int]*rpc.Client)
	myNode.put = make(chan putRequest)
	myNode.get = make(chan getRequest)
	myNode.getPaxosState = make(chan getPaxosStateRequest)
	myNode.putPaxosState = make(chan putPaxosStateRequest)

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
			if nodeID == srvId {
				node = myHostPort
			}
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
	delta := 100
	responseChan := make(chan getPaxosStateResponse)
	pn.getPaxosState <- getPaxosStateRequest{args.Key, responseChan}
	response := <-responseChan
	if !response.ok {
		reply.N = delta + pn.id
	} else {
		reply.N = response.state.minProposal - (response.state.minProposal % delta) + delta + pn.id
	}
	// fmt.Println(args.Key, reply.N)
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
	// fmt.Println("Node:", pn.id, "Propose", *args)
	majorityThreshold := int(1 + math.Floor(float64(len(pn.nodes))/float64(2)))
	resultChannel := make(chan error)
	go func() {
		highestAcceptedProposal := -1
		acceptedValue := args.V
		for {
			prepared := 0
			for nodeID, node := range pn.nodes {
				prepareArgs := paxosrpc.PrepareArgs{args.Key, args.N, pn.id}
				prepareReply := new(paxosrpc.PrepareReply)
				err := node.Call("PaxosNode.RecvPrepare", prepareArgs, prepareReply)
				if err != nil {
					fmt.Println("Failed to call RecvPrepare on node:", nodeID)
					continue
				}
				// fmt.Println("Node:", pn.id, "From:", nodeID, "PrepareReply", *prepareReply)
				if prepareReply.Status == paxosrpc.OK {
					prepared++
				}
				if prepareReply.N_a != -1 && prepareReply.N_a > highestAcceptedProposal {
					highestAcceptedProposal = prepareReply.N_a
					acceptedValue = prepareReply.V_a
				}
			}
			if prepared < majorityThreshold {
				time.Sleep(time.Duration(1+rand.Intn(3)) * time.Second)
				proposalNumberArgs := paxosrpc.ProposalNumberArgs{args.Key}
				proposalNumberReply := new(paxosrpc.ProposalNumberReply)
				err := pn.GetNextProposalNumber(&proposalNumberArgs, proposalNumberReply)
				if err != nil {
					fmt.Println("Failed to call GetNextProposalNumber on myself")
					continue
				}
				// fmt.Println("Failed to prepare a majority. Choosing new proposal", args.N, proposalNumberReply.N)
				args.N = proposalNumberReply.N
				continue
			}
			accepted := 0
			for nodeID, node := range pn.nodes {
				acceptArgs := paxosrpc.AcceptArgs{args.Key, args.N, acceptedValue, pn.id}
				acceptReply := new(paxosrpc.AcceptReply)
				err := node.Call("PaxosNode.RecvAccept", acceptArgs, acceptReply)
				if err != nil {
					fmt.Println("Failed to call RecvAccept on node:", nodeID)
					continue
				}
				// fmt.Println("Node:", nodeID, "AcceptReply", *acceptReply)
				if acceptReply.Status == paxosrpc.OK {
					accepted++
				}
			}
			if accepted < majorityThreshold {
				time.Sleep(time.Duration(1+rand.Intn(3)) * time.Second)
				proposalNumberArgs := paxosrpc.ProposalNumberArgs{args.Key}
				proposalNumberReply := new(paxosrpc.ProposalNumberReply)
				err := pn.GetNextProposalNumber(&proposalNumberArgs, proposalNumberReply)
				if err != nil {
					fmt.Println("Failed to call GetNextProposalNumber on myself")
					continue
				}
				// fmt.Println("Failed to accept a majority. Choosing new proposal", args.N, proposalNumberReply.N)
				args.N = proposalNumberReply.N
			} else {
				break
			}
		}
		for nodeID, node := range pn.nodes {
			commitArgs := paxosrpc.CommitArgs{args.Key, acceptedValue, pn.id}
			commitReply := new(paxosrpc.CommitReply)
			err := node.Call("PaxosNode.RecvCommit", commitArgs, commitReply)
			if err != nil {
				fmt.Println("Failed to call RecvCommit on node:", nodeID)
				continue
			}
		}
		reply.V = acceptedValue
		resultChannel <- nil
	}()
	select {
	case result := <-resultChannel:
		return result
	case <-time.After(PROPOSE_TIMEOUT):
		return errors.New("Propose timed out")
	}

}

// Desc:
// GetValue looks up the value for a key, and replies with the value or with
// the Status KeyNotFound.
//
// Params:
// args: the key to check
// reply: the value and status for this lookup of the given key
func (pn *paxosNode) GetValue(args *paxosrpc.GetValueArgs, reply *paxosrpc.GetValueReply) error {
	responseChan := make(chan getResponse)
	pn.get <- getRequest{args.Key, responseChan}
	response := <-responseChan
	if response.ok {
		reply.Status = paxosrpc.KeyFound
		reply.V = response.value
	} else {
		reply.Status = paxosrpc.KeyNotFound
		reply.V = nil
	}
	// fmt.Println("Node: Get", pn.id, *reply)
	return nil
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
	responseChan := make(chan getPaxosStateResponse)
	pn.getPaxosState <- getPaxosStateRequest{args.Key, responseChan}
	response := <-responseChan
	if !response.ok {
		pn.putPaxosState <- putPaxosStateRequest{args.Key, paxosState{args.N, -1, nil}}
		reply.Status = paxosrpc.OK
		reply.N_a = -1
		reply.V_a = nil
		return nil
	}
	if args.N > response.state.minProposal {
		pn.putPaxosState <- putPaxosStateRequest{args.Key, paxosState{args.N, response.state.acceptedProposal, response.state.acceptedValue}}
		reply.Status = paxosrpc.OK
	} else {
		reply.Status = paxosrpc.Reject
	}
	reply.N_a = response.state.acceptedProposal
	reply.V_a = response.state.acceptedValue
	return nil
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
	responseChan := make(chan getPaxosStateResponse)
	pn.getPaxosState <- getPaxosStateRequest{args.Key, responseChan}
	response := <-responseChan
	if !response.ok {
		reply.Status = paxosrpc.Reject
	} else if args.N >= response.state.minProposal {
		pn.putPaxosState <- putPaxosStateRequest{args.Key, paxosState{args.N, args.N, args.V}}
		reply.Status = paxosrpc.OK
	} else {
		reply.Status = paxosrpc.Reject
	}
	return nil
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
	responseChan := make(chan getPaxosStateResponse)
	pn.getPaxosState <- getPaxosStateRequest{args.Key, responseChan}
	response := <-responseChan
	if !response.ok {
		return errors.New("Trying to commit unprepared value")
	} else if response.state.acceptedValue != args.V {
		return errors.New("Trying to commit unaccepted value")
	}
	pn.put <- putRequest{args.Key, args.V}
	pn.putPaxosState <- putPaxosStateRequest{args.Key, paxosState{response.state.minProposal, -1, nil}}
	return nil
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
		case put := <-pn.putPaxosState:
			paxosStore[put.key] = put.value
		}
	}
}
