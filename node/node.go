package main

import (
	"fmt"
	"net"
	"os"
	"strconv"

	"google.golang.org/grpc"
)

func main() {
	id, _ := strconv.Atoi(os.Args[1])
	nodePorts := []string{"1001", "1002", "1003"}
	myport := nodePorts[id]

	localAddress, _ := net.ResolveUDPAddr("udp", "localhost"+myport)
	serverConnection, _ := net.ListenUDP("udp", localAddress)
	defer serverConnection.Close()

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

	fmt.Scan()
}

func criticalSection(id int) {
	fmt.Println(id, "has accessed the critical section.")
}
