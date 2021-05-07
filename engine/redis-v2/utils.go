package redis_v2

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/bitleak/lmstfy/uuid"
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

// GenTimerManagerID generates a consumer ID by local ipv4 address, current time and
// a UUID.
func genTimerManagerID() (string, error) {
	ipv4, err := getLocalIPv4()
	if err != nil {
		return "", err
	}
	currentMilliSec := time.Now().UnixNano() / int64(time.Millisecond)
	return fmt.Sprintf("{%d}-{%s}-{%s}", currentMilliSec, ipv4.String(), uuid.GenUniqueID()), nil
}

func getLocalIPv4() (net.IP, error) {
	netInterfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	for _, netInterface := range netInterfaces {
		if (netInterface.Flags & net.FlagUp) == 0 {
			continue
		}
		addrs, _ := netInterface.Addrs()
		for _, addr := range addrs {
			ipnet, ok := addr.(*net.IPNet)
			if !ok || ipnet.IP.IsLoopback() {
				continue
			}
			ipv4 := ipnet.IP.To4()
			if ipv4 == nil {
				continue
			}
			if ipv4[0] == 10 || (ipv4[0] == 172 && ipv4[1] >= 16 && ipv4[1] <= 31) || (ipv4[0] == 192 && ipv4[1] == 168) {
				return ipv4, nil
			}
		}
	}
	return nil, fmt.Errorf("no ip address satisfied requirement")
}
