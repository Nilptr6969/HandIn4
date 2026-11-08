package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	pb "project-root/grpc"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Pool struct {
	pb.UnimplementedBroadcastServer
	nodeID      int32                   // between 0-2
	logicalTime int32                   // the node's current time
	requestTime int32                   // the time for the request sent by this node
	requesting  bool                    // wether or not the node is requesting to enter the critical section
	deferred    map[int32]chan struct{} // the other nodes that are requesting but are being postponed while this node has priority
	// the channel is to signal to other nodes when their request can continue
	connections map[int32]pb.BroadcastClient // the other nodes
	mu          sync.Mutex
}

// when a node recieves a request
func (p *Pool) Request(ctx context.Context, request *pb.Time) (*pb.Time, error) {
	log.Println("Timestamp:", p.logicalTime, "Node", p.nodeID, "has recieved a request to enter the critical section")
	p.mu.Lock()
	if request.Timestamp > p.logicalTime {
		p.logicalTime = request.Timestamp
	}
	p.logicalTime++

	//if this node is also requesting
	//if so, then is it earlier than the requesting one, else if it's the same time who's id is smaller
	if p.requesting && (p.requestTime < request.Timestamp || (p.requestTime == request.Timestamp && p.nodeID < request.NodeId)) {
		granted := make(chan struct{})
		p.deferred[request.NodeId] = granted
		p.mu.Unlock()

		<-granted // this channel is closed when this node uses release after entering the critical section

		p.mu.Lock()
	}

	p.logicalTime++
	reply := &pb.Time{Timestamp: p.logicalTime, NodeId: p.nodeID}
	p.mu.Unlock()
	return reply, nil
}

// this node requesting, returns true when all other nodes have responded. only returns false if an error occurs
func (p *Pool) requestPermission() {
	log.Println("Timestamp:", p.logicalTime, "Node", p.nodeID, "is requesting permission to enter the critical section")
	p.mu.Lock()
	p.logicalTime++
	p.requesting = true
	p.requestTime = p.logicalTime
	request := &pb.Time{Timestamp: p.requestTime, NodeId: p.nodeID}
	p.mu.Unlock()

	replies := make(chan bool, len(p.connections))
	for _, connection := range p.connections {
		go p.sendRequest(connection, request, replies)
	}

	for range len(p.connections) {
		<-replies // makes shure that all replies must be recieved before the node continues to the critical section
		log.Println("Timestamp:", p.logicalTime, "Node", p.nodeID, "has recieved a reply from another node")
	}
	log.Println("Timestamp:", p.logicalTime, "Node", p.nodeID, "has been granted permission to enter the critical section")
}

// makes a request for each of the other nodes. It is asyncronous so that we can have a channel waiting for the response of each node
func (p *Pool) sendRequest(connection pb.BroadcastClient, request *pb.Time, replies chan bool) {
	log.Println("Timestamp:", p.logicalTime, "Node", p.nodeID, "is sending a request")
	reply, _ := connection.Request(context.Background(), request, grpc.WaitForReady(true))

	p.mu.Lock()
	if reply.Timestamp > p.logicalTime {
		p.logicalTime = reply.Timestamp
	}
	p.logicalTime++
	p.mu.Unlock()
	replies <- true
}

func (p *Pool) release() { //resets the node
	log.Println("Timestamp:", p.logicalTime, "Node", p.nodeID, "is releasing access to the critical section")
	p.mu.Lock()
	p.requesting = false
	p.logicalTime++
	deferred := p.deferred
	p.deferred = make(map[int32]chan struct{})
	p.mu.Unlock()

	for _, granted := range deferred {
		close(granted)
	}
	log.Println("Timestamp:", p.logicalTime, "Node", p.nodeID, "has replied to all nodes requesting access to the critical section")
}

func (p *Pool) runCriticalSection() {
	p.requestPermission()
	defer p.release()
	log.Println("Timestamp:", p.logicalTime, "Node", p.nodeID, "is entering critical section")
	p.enterCriticalSection()
	log.Println("Timestamp:", p.logicalTime, "Node", p.nodeID, "is out of critical section")
}

func main() {
	// Logging taken from https://last9.io/blog/write-logs-to-file/
	logFile, err := os.OpenFile("app.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer logFile.Close()

	log.SetOutput(logFile)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	id, _ := strconv.Atoi(os.Args[1])

	nodePorts := []string{"50051", "50052", "50053"}
	pool := &Pool{nodeID: int32(id), deferred: make(map[int32]chan struct{}), connections: make(map[int32]pb.BroadcastClient)}
	server := grpc.NewServer()
	pb.RegisterBroadcastServer(server, pool)

	for peerID, port := range nodePorts {
		if peerID == id {
			continue
		}
		connection, _ := grpc.NewClient("localhost:"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
		defer connection.Close()
		pool.connections[int32(peerID)] = pb.NewBroadcastClient(connection)
	}

	listener, _ := net.Listen("tcp", "localhost:"+nodePorts[id])

	go server.Serve(listener)

	for {
		fmt.Print("Enter a message to request the critical section: ")
		if !bufio.NewScanner(os.Stdin).Scan() {
			return
		}
		pool.runCriticalSection()
	}
}

func (p *Pool) enterCriticalSection() {
	log.Println("Timestamp:", p.logicalTime, "Node", p.nodeID, "is in critical section")
	fmt.Println(p.nodeID, "has accessed the critical section.")
	time.Sleep(5 * time.Second)
	log.Println("Timestamp:", p.logicalTime, "Node", p.nodeID, "is done in critical section")
}
