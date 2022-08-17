package conf

type SpannerConfig struct {
	Project  string `mapstructure:"project" validate:"required"`
	Instance string `mapstructure:"instance" validate:"required"`
	Database string `mapstructure:"db" validate:"required"`

	S3Bucket  string `mapstructure:"s3_bucket"`
	S3FileKey string `mapstructure:"s3_file_key"`
	// for local test
	CredentialsFile string `mapstructure:"credentials_file"`
}
