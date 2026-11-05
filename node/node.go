package main

import (
	"fmt"
	"net"
	"os"
	"strconv"

	pb "project-root/grpc"

	"google.golang.org/grpc"
)

func main() {
	id, _ := strconv.Atoi(os.Args[1])
	nodePorts := []string{"1001", "1002", "1003"}
	myport := nodePorts[id]

	myServer := grpc.NewServer()
	pb.RegisterBroadcastServer(myServer, pb.UnimplementedBroadcastServer{})

	connections := make([]*grpc.ClientConn, 2)
	for i, port := range nodePorts {
		if i == id {
			continue
		}
		client, _ := grpc.NewClient(port)
		connections := append(connections, client)
		if i > id {
			defer connections[i-1].Close()
		} else {
			defer connections[i].Close()
		}
	}

	listener, _ := net.Listen("tcp", "localhost:"+myport)
	myServer.Serve(listener)
}

func criticalSection(id int) {
	fmt.Println(id, "has accessed the critical section.")
}
