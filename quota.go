package quota

import (
	"context"
	"math/rand"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var ()

const (
	QuotaProjectKey = "quotaproject"
	quotaHeaderKey  = "X-Goog-User-Project"
)

type ClientMetadataKey string

type QuotaHandlerConfig struct {
	Projects []string
}

func NewQuotaUnaryHandler(conf *QuotaHandlerConfig) grpc.UnaryClientInterceptor {
	rand.Seed(time.Now().UnixNano())

	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		send, _ := metadata.FromOutgoingContext(ctx)
		var targetQuotaProject string
		if ctx.Value(ClientMetadataKey(QuotaProjectKey)) != nil {
			if n, ok := ctx.Value(ClientMetadataKey(QuotaProjectKey)).(string); ok {
				targetQuotaProject = n
			}
		} else {
			targetQuotaProject = conf.Projects[rand.Intn(len(conf.Projects))]
		}
		send.Set(quotaHeaderKey, targetQuotaProject)
		newCtx := metadata.NewOutgoingContext(ctx, send)
		err := invoker(newCtx, method, req, reply, cc, opts...)

		return err
	}
}

func NewQuotaStreamingHandler(conf *QuotaHandlerConfig) grpc.StreamClientInterceptor {
	rand.Seed(time.Now().UnixNano())

	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		send, _ := metadata.FromOutgoingContext(ctx)
		var targetQuotaProject string
		if ctx.Value(ClientMetadataKey(QuotaProjectKey)) != nil {
			if n, ok := ctx.Value(ClientMetadataKey(QuotaProjectKey)).(string); ok {
				targetQuotaProject = n
			}
		} else {
			targetQuotaProject = conf.Projects[rand.Intn(len(conf.Projects))]
		}
		send.Set(quotaHeaderKey, targetQuotaProject)
		newCtx := metadata.NewOutgoingContext(ctx, send)

		return streamer(newCtx, desc, cc, method, opts...)
	}
}
