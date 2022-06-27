package main

import (
	"context"
	"fmt"
	"log"
	"net"

	api "github.com/grpc-test/proto"
	"google.golang.org/grpc"
)

type S struct {
	api.UnimplementedHelloServer
}

func main() {
	lis, err := net.Listen("tcp", "localhost:1231")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	api.RegisterHelloServer(s, &S{})

	fmt.Println("gRPC start on port 1231")
	s.Serve(lis)
}

func (s *S) Hello(ctx context.Context, in *api.Input) (*api.Output, error) {

	fmt.Println("Hello", in.GetName())

	return &api.Output{
		Name: in.GetName(),
	}, nil
}
