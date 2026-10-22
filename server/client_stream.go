package main

import (
	"errors"
	"io"
	"log"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/akhil/grpc-demo/proto"
)

func (s *helloServer) SayHelloClientStreaming(stream pb.GreetService_SayHelloClientStreamingServer) error {
	var messages []string

	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			// Client finished sending
			if len(messages) == 0 {
				return status.Error(codes.InvalidArgument, "at least one name is required")
			}
			return stream.SendAndClose(&pb.MessagesList{Messages: messages})
		}
		if err != nil {
			// Includes client cancellation; the error already carries a status code
			return err
		}

		if req.GetName() == "" {
			return status.Error(codes.InvalidArgument, "name must not be empty")
		}

		log.Printf("Got request with name: %s", req.GetName())
		messages = append(messages, "Hello "+req.GetName())
	}
}