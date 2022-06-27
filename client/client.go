package main

import (
	"context"
	"fmt"
	"log"

	api "github.com/grpc-test/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.Dial("localhost:1231", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	cli := api.NewHelloClient(conn)

	out, err := cli.Hello(context.TODO(), &api.Input{
		Name: "Test",
	})
	if err != nil {
		log.Panic(err)
	}

	fmt.Println("returned", out.GetName())
}
