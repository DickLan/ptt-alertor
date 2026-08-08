package connections

import (
	"fmt"
	"os"
	"time"

	"github.com/garyburd/redigo/redis"
)

var pool = newPool()

func address() string {
	host := os.Getenv("REDIS_ENDPOINT")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("REDIS_PORT")
	if port == "" {
		port = "6379"
	}
	return host + ":" + port
}

func newPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		MaxActive:   50,
		IdleTimeout: 300 * time.Second,
		Wait:        true,
		Dial: func() (redis.Conn, error) {
			return redis.Dial(
				"tcp",
				address(),
				redis.DialConnectTimeout(5*time.Second),
				redis.DialReadTimeout(5*time.Second),
				redis.DialWriteTimeout(5*time.Second),
			)
		},
		TestOnBorrow: func(conn redis.Conn, lastUsed time.Time) error {
			if time.Since(lastUsed) < time.Minute {
				return nil
			}
			_, err := conn.Do("PING")
			return err
		},
	}
}

// Redis get redis connection
func Redis() redis.Conn {
	return pool.Get()
}

// RedisPubSub returns a dedicated connection without a read timeout because
// Pub/Sub receive calls are expected to wait while no messages are published.
func RedisPubSub() (redis.Conn, error) {
	return redis.Dial(
		"tcp",
		address(),
		redis.DialConnectTimeout(5*time.Second),
		redis.DialWriteTimeout(5*time.Second),
	)
}

// Ping checks whether the configured Redis instance is available.
func Ping() error {
	conn := Redis()
	defer conn.Close()
	result, err := redis.String(conn.Do("PING"))
	if err != nil {
		return err
	}
	if result != "PONG" {
		return fmt.Errorf("unexpected Redis PING response: %s", result)
	}
	return nil
}

// Close releases the shared Redis pool during process shutdown.
func Close() error {
	return pool.Close()
}
