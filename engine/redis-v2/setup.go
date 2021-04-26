package redis_v2

import (
	"context"

	"github.com/sirupsen/logrus"
)

var (
	logger   *logrus.Logger
	dummyCtx = context.TODO()
)
