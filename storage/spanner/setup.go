package spanner

import (
	"context"
	"fmt"

	"github.com/AfterShip/gopkg/storage/s3"
	"github.com/AfterShip/gopkg/storage/spannerx"
	"google.golang.org/api/option"

	"github.com/bitleak/lmstfy/storage/conf"
)

func CreateSpannerClient(cfg *conf.SpannerConfig) (*spannerx.Client, error) {
	database := fmt.Sprintf("projects/%s/instances/%s/databases/%s", cfg.Project, cfg.Instance, cfg.Database)
	if cfg.CredentialsFile != "" {
		opt := option.WithCredentialsFile(cfg.CredentialsFile)
		return spannerx.NewClient(context.Background(), database, opt)
	} else if cfg.S3Bucket != "" && cfg.S3FileKey != "" {
		fileBytes, err := s3.ReadFile(context.Background(), cfg.S3Bucket, cfg.S3FileKey)
		if err != nil {
			return nil, fmt.Errorf("read s3 file: %w", err)
		}
		opt := option.WithCredentialsJSON(fileBytes)
		return spannerx.NewClient(context.Background(), database, opt)
	}
	return spannerx.NewClient(context.Background(), database)
}
