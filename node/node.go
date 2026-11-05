package main

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	pb "grpc"
	"strconv"
	"sync"

	"log"

	"google.golang.org/grpc"
)

func main(){
	grpc.NewServer("localhost:5050", grpc.WithTransportCredentials(insecure.NewCredentials()))
}