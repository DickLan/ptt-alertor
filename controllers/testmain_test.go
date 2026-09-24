package controllers

import (
	"net"
	"os"
	"testing"

	"github.com/alicebob/miniredis"
)

var controllerRedis *miniredis.Miniredis

func TestMain(m *testing.M) {
	server, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	controllerRedis = server
	host, port, err := net.SplitHostPort(server.Addr())
	if err != nil {
		panic(err)
	}
	_ = os.Setenv("REDIS_ENDPOINT", host)
	_ = os.Setenv("REDIS_PORT", port)
	_ = os.Setenv("STORAGE_BACKEND", "redis")
	code := m.Run()
	server.Close()
	os.Exit(code)
}
