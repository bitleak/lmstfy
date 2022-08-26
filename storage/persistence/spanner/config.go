package spanner

type SpannerConfig struct {
	Project         string `mapstructure:"project" validate:"required"`
	Instance        string `mapstructure:"instance" validate:"required"`
	Database        string `mapstructure:"db" validate:"required"`
	CredentialsFile string `mapstructure:"credentials_file"`
	TableName       string
}
