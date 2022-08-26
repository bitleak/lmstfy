package spanner

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/option"

	"github.com/bitleak/lmstfy/config"
)

func CreateSpannerClient(cfg *config.SpannerConfig) (*spanner.Client, error) {
	db := fmt.Sprintf("projects/%s/instances/%s/databases/%s", cfg.Project, cfg.Instance, cfg.Database)
	if cfg.CredentialsFile != "" {
		opt := option.WithCredentialsFile(cfg.CredentialsFile)
		return spanner.NewClient(context.Background(), db, opt)
	}
	return spanner.NewClient(context.Background(), db)
}
