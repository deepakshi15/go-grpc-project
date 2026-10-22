package main

import (
	"context"
	"io"
	"log"
	"errors"
	"time"
	pb "github.com/akhil/grpc-demo/proto"
)

func callSayHelloServerStream(client pb.GreetServiceClient, names *pb.NamesList) {
	log.Println("Server streaming started")

	// Give the whole call a deadline
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := client.SayHelloServerStreaming(ctx, names)
	if err != nil {
		logRPCError("server streaming", err)
		return
	}

	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			logRPCError("server streaming", err)
			return
		}
		log.Println(msg.GetMessage())
	}
	log.Println("Server streaming finished")
}
