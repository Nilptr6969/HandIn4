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

		<-granted

		p.mu.Lock()
	}

	p.logicalTime++
	reply := &pb.Time{Timestamp: p.logicalTime, NodeId: p.nodeID}
	p.mu.Unlock()
	return reply, nil
}

// this node requesting, returns true when all other nodes have responded. only returns false if an error occurs
func (p *Pool) requestPermission() bool {
	p.mu.Lock()
	p.logicalTime++
	p.requesting = true
	p.requestTime = p.logicalTime
	request := &pb.Time{Timestamp: p.requestTime, NodeId: p.nodeID}
	p.mu.Unlock()

	replies := make(chan bool, len(p.connections))
	for _, connection := range p.connections {
		go func(connection pb.BroadcastClient) { // makes a request for each of the other nodes. It is asyncronous so that we can have a channel waiting for the response of each node
			reply, err := connection.Request(context.Background(), request, grpc.WaitForReady(true))
			if err != nil || reply == nil { // in case of error or no reply
				replies <- false
				return
			}
			p.mu.Lock()

			if reply.Timestamp > p.logicalTime {
				p.logicalTime = reply.Timestamp
			}
			p.logicalTime++
			p.mu.Unlock()
			replies <- true
		}(connection)
	}

	granted := true // whether or not the critical section can be accessed
	for range len(p.connections) {
		if !<-replies { // if any reply fails then granted will be false and the critical section cannot be entered
			granted = false
		}
	}
	return granted
}

func (p *Pool) release() { //resets the node
	p.mu.Lock()
	p.requesting = false
	p.logicalTime++
	p.deferred = make(map[int32]chan struct{})
	p.mu.Unlock()

	for _, granted := range p.deferred {
		close(granted)
	}
}

func (p *Pool) runCriticalSection() {
	if !p.requestPermission() {
		p.release()
		return
	}
	defer p.release()
	log.Println("Node", p.nodeID, "is entering critical section")
	criticalSection(int(p.nodeID))
	log.Println("Node", p.nodeID, "is out of critical section")
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

	id, _ := strconv.Atoi(os.Args[0])

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

func criticalSection(id int) {
	log.Println("Node", id, "is in critical section")
	fmt.Println(id, "has accessed the critical section.")
	time.Sleep(10 * time.Second)
	log.Println("Node", id, "is done in critical section")
}
