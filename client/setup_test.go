package client

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/meitu/lmstfy/config"
	"github.com/meitu/lmstfy/helper"
)

var (
	CONF *config.Config
)

var (
	Host      string
	Port      int
	Namespace = "client-ns"
	Token     string
)

func init() {
	cfg := os.Getenv("LMSTFY_TEST_CONFIG")
	if cfg == "" {
		panic(`
############################################################
PLEASE setup env LMSTFY_TEST_CONFIG to the config file first
############################################################
`)
	}
	var err error
	if CONF, err = config.MustLoad(os.Getenv("LMSTFY_TEST_CONFIG")); err != nil {
		panic(fmt.Sprintf("Failed to load config file: %s", err))
	}
}

// NOTE: lmstfy server should be start by gitlab CI script from outside, but should use the same
// config file specified in $LMSTFY_TEST_CONFIG
func setup() {
	Host = CONF.Host
	Port = CONF.Port
	adminPort := CONF.AdminPort

	// Flush redis DB
	for _, poolConf := range CONF.Pool {
		conn := helper.NewRedisClient(&poolConf, nil)
		err := conn.Ping().Err()
		if err != nil {
			panic(fmt.Sprintf("Failed to ping: %s", err))
		}
		err = conn.FlushDB().Err()
		if err != nil {
			panic(fmt.Sprintf("Failed to flush db: %s", err))
		}
	}

	// Create the token first
	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/token/%s?description=client", adminPort, Namespace), nil)
	if err != nil {
		panic("Failed to create testing token")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic("Failed to create testing token")
	}
	if resp.StatusCode != http.StatusCreated {
		panic("Failed to create testing token")
	}
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic("Failed to create testing token")
	}
	var respData struct {
		Token string `json:"token"`
	}
	err = json.Unmarshal(respBytes, &respData)
	if err != nil {
		panic("Failed to create testing token")
	}
	Token = respData.Token
}

func teardown() {}

func TestMain(m *testing.M) {
	setup()
	ret := m.Run()
	teardown()
	os.Exit(ret)
}
