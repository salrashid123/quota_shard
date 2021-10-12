package main

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	quota "github.com/salrashid123/quota_shard"

	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

var (
	projectID     = "qprojecta"
	quotaProjects = []string{"qprojectb", "qprojectc"}
)

func main() {

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID, option.WithGRPCDialOption(grpc.WithUnaryInterceptor(quota.NewQuotaUnaryHandler(&quota.QuotaHandlerConfig{
		Projects: quotaProjects,
	}))), option.WithGRPCDialOption(grpc.WithStreamInterceptor(quota.NewQuotaStreamingHandler(&quota.QuotaHandlerConfig{
		Projects: quotaProjects,
	}))))
	if err != nil {
		fmt.Printf("Could not create pubsub Client: %v", err)
		return
	}

	// // this will directly apply quota to a given project
	// newCtx := context.WithValue(ctx, quota.ClientMetadataKey(quota.QuotaProjectKey), "PROJECT_B")
	// topics := client.Topics(newCtx)
	// for {
	// 	topic, err := topics.Next()
	// 	if err == iterator.Done {
	// 		break
	// 	}
	// 	if err != nil {
	// 		fmt.Printf("Error listing topics %v", err)
	// 		return
	// 	}
	// 	fmt.Println(topic)
	// }

	// this will distribute quota randomly
	for {
		topics := client.Topics(ctx)
		for {
			topic, err := topics.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				fmt.Printf("Error listing topics %v", err)
				return
			}
			fmt.Println(topic)
		}
		time.Sleep(1000 * time.Millisecond)
	}

}
