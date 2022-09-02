package storage

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	go_redis "github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	instancepb "google.golang.org/genproto/googleapis/spanner/admin/instance/v1"
	"google.golang.org/grpc/codes"

	"github.com/bitleak/lmstfy/config"
	"github.com/bitleak/lmstfy/helper"
	"github.com/bitleak/lmstfy/log"
)

var (
	testConfig *config.PresetConfigForTest
	logger     *logrus.Logger
	VersionV2  = "v2"
)

type RedisInstance struct {
	Name string
	Conn *go_redis.Client
}

func setup(CONF *config.Config) {
	logger = logrus.New()
	level, _ := logrus.ParseLevel(CONF.LogLevel)
	logger.SetLevel(level)

	log.Setup(CONF.LogFormat, CONF.LogDir, CONF.LogLevel, "ERROR")
	poolConf := CONF.Pool["default"]
	conn := helper.NewRedisClient(&poolConf, nil)
	err := conn.Ping(dummyCtx).Err()
	if err != nil {
		panic(fmt.Sprintf("Failed to ping: %s", err))
	}
	err = conn.FlushDB(dummyCtx).Err()
	if err != nil {
		panic(fmt.Sprintf("Failed to flush db: %s", err))
	}
}

func TestMain(m *testing.M) {
	presetConfig, err := config.CreatePresetForTest(VersionV2)
	if err != nil {
		panic(fmt.Sprintf("CreatePresetForTest failed with error: %s", err))
	}
	defer presetConfig.Destroy()
	setup(presetConfig.Config)
	testConfig = presetConfig
	ret := m.Run()
	os.Exit(ret)
}

func CreateInstance(ctx context.Context, cfg *config.SpannerConfig) error {
	instanceName := fmt.Sprintf("projects/%s/instances/%s", cfg.Project, cfg.Instance)

	instanceAdminClient, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		return err
	}
	defer instanceAdminClient.Close()

	_, err = instanceAdminClient.GetInstance(ctx, &instancepb.GetInstanceRequest{
		Name: instanceName,
	})
	if err != nil && spanner.ErrCode(err) != codes.NotFound {
		return err
	}
	if err == nil {
		return nil
	}

	_, err = instanceAdminClient.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     "projects/" + cfg.Project,
		InstanceId: cfg.Instance,
	})
	return err
}

func CreateDatabase(ctx context.Context, cfg *config.SpannerConfig) error {
	databaseAdminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}
	instanceName := fmt.Sprintf("projects/%s/instances/%s", cfg.Project, cfg.Instance)
	dbName := fmt.Sprintf("%s/databases/%s", instanceName, cfg.Database)
	_, err = databaseAdminClient.GetDatabase(ctx, &databasepb.GetDatabaseRequest{Name: dbName})
	if err != nil && spanner.ErrCode(err) != codes.NotFound {
		return err
	}
	if err == nil {
		// db exists
		return nil
	}

	ddlBytes, err := ioutil.ReadFile("../scripts/schemas/spanner/ddls.sql")
	if err != nil {
		return fmt.Errorf("read ddls file: %w", err)
	}
	ddls := make([]string, 0)
	for _, ddl := range strings.Split(string(ddlBytes), ";") {
		ddl = strings.TrimSpace(ddl)
		if len(ddl) != 0 {
			ddls = append(ddls, ddl)
		}
	}
	op, err := databaseAdminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          instanceName,
		CreateStatement: "CREATE DATABASE `" + cfg.Database + "`",
		ExtraStatements: ddls,
	})
	if err != nil {
		return err
	}
	_, err = op.Wait(ctx)
	return err
}
