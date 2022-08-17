package spanner

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"

	"github.com/bitleak/lmstfy/storage/conf"
)

func CreateSpannerClient(cfg *conf.SpannerConfig) (*spanner.Client, error) {
	db := fmt.Sprintf("projects/%s/instances/%s/databases/%s", cfg.Project, cfg.Instance, cfg.Database)
	spannerClient, err := spanner.NewClient(context.Background(), db)
	if err != nil {
		fmt.Printf("failed to create spanner client with error: %v", err)
		return nil, err
	}
	return spannerClient, nil
}
