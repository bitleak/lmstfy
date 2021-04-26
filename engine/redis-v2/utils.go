package redis_v2

import "strings"

func join(args ...string) string {
	return strings.Join(args, "/")
}

func splits(n int, s string) []string {
	return strings.SplitN(s, "/", n)
}
