package main

import (
	"context"
	"log"
	"time"

	pb "github.com/mohammad-farrokhnia/go-ingestor/proto/ingestor/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// 1. Connect to the server (no SSL for now)
	conn, err := grpc.NewClient("127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	c := pb.NewIngestorServiceClient(conn)

	// 2. Send an Event
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	log.Println("Sending event...")
	r, err := c.Ingest(ctx, &pb.IngestRequest{
		EventId:   "evt-12345",
		Source:    "test-client",
		Payload:   `{"user_id": 10, "action": "click"}`,
		Timestamp: time.Now().Unix(),
	})

	if err != nil {
		log.Fatalf("could not ingest: %v", err)
	}

	log.Printf("Response: Status=%s, Error=%s", r.Status, r.Error)
}
