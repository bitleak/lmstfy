package spanner

import (
	"context"
	"fmt"
	"google.golang.org/api/option"

	"cloud.google.com/go/spanner"

	"github.com/bitleak/lmstfy/storage/conf"
)

func CreateSpannerClient(cfg *conf.SpannerConfig) (*spanner.Client, error) {
	db := fmt.Sprintf("projects/%s/instances/%s/databases/%s", cfg.Project, cfg.Instance, cfg.Database)
	if cfg.CredentialsFile != "" {
		opt := option.WithCredentialsFile(cfg.CredentialsFile)
		return spanner.NewClient(context.Background(), db, opt)
	}
	return spanner.NewClient(context.Background(), db)
}
