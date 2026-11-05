package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"

	pb "project-root/grpc"

	"google.golang.org/grpc"
)

type Pool struct {
	pb.UnimplementedBroadcastServer
	Connection []*grpc.ClientConn
}

func Request(ctx context.Context, time *pb.Time) {

}

func Response(time pb.Time) {

}

func main() {
	id, _ := strconv.Atoi(os.Args[1])
	nodePorts := []string{"1001", "1002", "1003"}
	myport := nodePorts[id]

	myServer := grpc.NewServer()

	var conn []*grpc.ClientConn
	pool := &Pool{
		Connection: conn,
	}
	pb.RegisterBroadcastServer(myServer, pool)

	for i, port := range nodePorts {
		if i == id {
			continue
		}
		client, _ := grpc.NewClient(port)
		pool.Connection = append(pool.Connection, client)
		if i > id {
			defer pool.Connection[i-1].Close()
		} else {
			defer pool.Connection[i].Close()
		}
	}

	listener, _ := net.Listen("tcp", "localhost:"+myport)
	myServer.Serve(listener)
}

func criticalSection(id int) {
	fmt.Println(id, "has accessed the critical section.")
}
