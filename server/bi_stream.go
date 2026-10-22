package main

import (
	"errors"
	"io"
	"log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/akhil/grpc-demo/proto"
)

func (s *helloServer) SayHelloBidirectionalStreaming(stream pb.GreetService_SayHelloBidirectionalStreamingServer) error {
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}

		if req.GetName() == "" {
			return status.Error(codes.InvalidArgument, "name must not be empty")
		}

		log.Printf("Got request with name: %s", req.GetName())
		if err := stream.Send(&pb.HelloResponse{Message: "Hello " + req.GetName()}); err != nil {
			return err
		}
	}
}