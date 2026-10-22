package main

import (
	"log"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/akhil/grpc-demo/proto"
)

func (s *helloServer) SayHelloServerStreaming(req *pb.NamesList, stream pb.GreetService_SayHelloServerStreamingServer) error {
	// Validate everything before sending anything
	if len(req.GetNames()) == 0 {
		return status.Error(codes.InvalidArgument, "names list must not be empty")
	}
	for _, name := range req.GetNames() {
		if name == "" {
			return status.Error(codes.InvalidArgument, "names must not be empty strings")
		}
	}

	log.Printf("Got request with names: %v", req.GetNames())
	ctx := stream.Context()

	for _, name := range req.GetNames() {
		if err := stream.Send(&pb.HelloResponse{Message: "Hello " + name}); err != nil {
			return err
		}

		// Wait 2 seconds, but stop immediately if the client cancels or times out
		select {
		case <-ctx.Done():
			log.Printf("Client cancelled the stream: %v", ctx.Err())
			return status.FromContextError(ctx.Err()).Err()
		case <-time.After(2 * time.Second):
		}
	}
	return nil
}