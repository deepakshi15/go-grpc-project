package main

import (
	"log"

	pb "github.com/akhil/grpc-demo/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	port = ":8080"
)

func main() {
	conn, err := grpc.NewClient("localhost"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewGreetServiceClient(conn)

	names := &pb.NamesList{
		Names: []string{"Akhil", "Alice", "Bob"},
	}

	//callSayHello(client)
	callSayHelloServerStream(client, names)
	//callSayHelloClientStream(client, names)
	//callSayHelloBidirectionalStream(client, names)
}

// logRPCError prints the gRPC status code and message from an error
func logRPCError(operation string, err error) {
	st, ok := status.FromError(err)
	if !ok {
		log.Printf("%s failed: %v", operation, err)
		return
	}
	log.Printf("%s failed: code=%s message=%s", operation, st.Code(), st.Message())
}