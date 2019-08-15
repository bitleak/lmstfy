package redis

import "testing"

func TestGetRedisInfo(t *testing.T) {
	R.Conn.Set("info", 1, 0)
	info := GetRedisInfo(R)
	if info.NKeys < 1 {
		t.Fatalf("Expected NKeys is at least 1")
	}
	if info.MemUsed <= 0 {
		t.Fatalf("Expected MemUsed is non-zero")
	}
	if info.NClients < 1 {
		t.Fatalf("Expected NClients is at least 1")
	}
}
