package redis_v2

import (
	"strings"
	"time"
)

func join(args ...string) string {
	return strings.Join(args, "/")
}

func splits(n int, s string) []string {
	return strings.SplitN(s, "/", n)
}

func isLuaScriptGone(err error) bool {
	return strings.HasPrefix(err.Error(), "NOSCRIPT")
}

func timeScore() int64 {
	return 1<<priorityShift - time.Now().UnixNano()/1e6
}
