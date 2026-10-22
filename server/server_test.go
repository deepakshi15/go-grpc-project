package main

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	pb "github.com/akhil/grpc-demo/proto"
)

const bufSize = 1024 * 1024

// newTestClient starts the real gRPC server on an in-memory connection
// and returns a client connected to it. Everything is cleaned up when the test ends.
func newTestClient(t *testing.T, delay time.Duration) pb.GreetServiceClient {
	t.Helper()

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	pb.RegisterGreetServiceServer(srv, &helloServer{delay: delay})

	go func() {
		_ = srv.Serve(lis)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		conn.Close()
		srv.Stop()
	})

	return pb.NewGreetServiceClient(conn)
}

// ---------- Unary ----------

func TestSayHello(t *testing.T) {
	client := newTestClient(t, 0)

	res, err := client.SayHello(context.Background(), &pb.NoParam{})

	require.NoError(t, err)
	assert.Equal(t, "Hello", res.GetMessage())
}

// ---------- Server streaming ----------

// receiveAll reads a server stream until it ends and returns every message.
func receiveAll(stream pb.GreetService_SayHelloServerStreamingClient) ([]string, error) {
	var messages []string
	for {
		res, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return messages, nil
		}
		if err != nil {
			return messages, err
		}
		messages = append(messages, res.GetMessage())
	}
}

func TestSayHelloServerStreaming(t *testing.T) {
	tests := []struct {
		name     string
		names    []string
		wantMsgs []string
		wantCode codes.Code
	}{
		{
			name:     "valid names",
			names:    []string{"Akhil", "Alice", "Bob"},
			wantMsgs: []string{"Hello Akhil", "Hello Alice", "Hello Bob"},
			wantCode: codes.OK,
		},
		{
			name:     "empty list",
			names:    []string{},
			wantMsgs: nil,
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "empty name in list",
			names:    []string{"Akhil", ""},
			wantMsgs: nil, // nothing is sent because validation happens first
			wantCode: codes.InvalidArgument,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newTestClient(t, 0)

			stream, err := client.SayHelloServerStreaming(context.Background(), &pb.NamesList{Names: tc.names})
			require.NoError(t, err)

			msgs, err := receiveAll(stream)

			assert.Equal(t, tc.wantCode, status.Code(err))
			assert.Equal(t, tc.wantMsgs, msgs)
		})
	}
}

func TestSayHelloServerStreaming_DeadlineExceeded(t *testing.T) {
	// 100 ms between messages, but the client only waits 150 ms in total
	client := newTestClient(t, 100*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	stream, err := client.SayHelloServerStreaming(ctx, &pb.NamesList{Names: []string{"Akhil", "Alice", "Bob"}})
	require.NoError(t, err)

	msgs, err := receiveAll(stream)

	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
	assert.Less(t, len(msgs), 3, "the stream should stop before all names are sent")
}

// ---------- Client streaming ----------

func TestSayHelloClientStreaming(t *testing.T) {
	tests := []struct {
		name     string
		names    []string
		wantMsgs []string
		wantCode codes.Code
	}{
		{
			name:     "valid names",
			names:    []string{"Akhil", "Alice"},
			wantMsgs: []string{"Hello Akhil", "Hello Alice"},
			wantCode: codes.OK,
		},
		{
			name:     "no names sent",
			names:    []string{},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "empty name",
			names:    []string{"Akhil", ""},
			wantCode: codes.InvalidArgument,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newTestClient(t, 0)

			stream, err := client.SayHelloClientStreaming(context.Background())
			require.NoError(t, err)

			for _, n := range tc.names {
				// If the server already rejected the stream, Send returns io.EOF;
				// the real error is returned by CloseAndRecv below.
				if err := stream.Send(&pb.HelloRequest{Name: n}); err != nil {
					break
				}
			}

			res, err := stream.CloseAndRecv()

			assert.Equal(t, tc.wantCode, status.Code(err))
			if tc.wantCode == codes.OK {
				assert.Equal(t, tc.wantMsgs, res.GetMessages())
			}
		})
	}
}

// ---------- Bidirectional streaming ----------

func TestSayHelloBidirectionalStreaming(t *testing.T) {
	tests := []struct {
		name     string
		names    []string
		wantMsgs []string
		wantCode codes.Code
	}{
		{
			name:     "valid names",
			names:    []string{"Akhil", "Alice", "Bob"},
			wantMsgs: []string{"Hello Akhil", "Hello Alice", "Hello Bob"},
			wantCode: codes.OK,
		},
		{
			name:     "empty name stops the stream",
			names:    []string{"Akhil", ""},
			wantMsgs: []string{"Hello Akhil"}, // the first reply arrives before the bad name
			wantCode: codes.InvalidArgument,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newTestClient(t, 0)

			stream, err := client.SayHelloBidirectionalStreaming(context.Background())
			require.NoError(t, err)

			for _, n := range tc.names {
				if err := stream.Send(&pb.HelloRequest{Name: n}); err != nil {
					break
				}
			}
			require.NoError(t, stream.CloseSend())

			var msgs []string
			for {
				res, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					err = nil
				}
				if err != nil || res == nil {
					assert.Equal(t, tc.wantCode, status.Code(err))
					break
				}
				msgs = append(msgs, res.GetMessage())
			}

			assert.Equal(t, tc.wantMsgs, msgs)
		})
	}
}