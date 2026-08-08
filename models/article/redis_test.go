package article

import (
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/garyburd/redigo/redis"
)

func TestRedisFindHandlesMissingAndCachedArticles(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer server.Close()

	original := connectRedis
	connectRedis = func() redis.Conn {
		conn, err := redis.Dial("tcp", server.Addr())
		if err != nil {
			t.Fatalf("dial miniredis: %v", err)
		}
		return conn
	}
	defer func() { connectRedis = original }()

	var missing Article
	Redis{}.Find("M.missing", &missing)
	if missing.Code != "" {
		t.Fatalf("missing article code = %q, want empty", missing.Code)
	}

	conn := connectRedis()
	_, err = conn.Do(
		"HMSET",
		prefix+"M.cached"+detailSuffix,
		"board", "NBA",
		"content", `{"code":"M.cached","Title":"cached"}`,
	)
	_ = conn.Close()
	if err != nil {
		t.Fatalf("seed cached article: %v", err)
	}

	var cached Article
	Redis{}.Find("M.cached", &cached)
	if cached.Code != "M.cached" || cached.Title != "cached" || cached.Board != "NBA" {
		t.Fatalf("cached article = %#v, want code/title/board restored", cached)
	}
}

func TestRedisFindEReturnsCorruptAndStorageErrors(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	original := connectRedis
	connectRedis = func() redis.Conn {
		conn, dialErr := redis.Dial("tcp", server.Addr())
		if dialErr != nil {
			t.Fatalf("dial miniredis: %v", dialErr)
		}
		return conn
	}
	defer func() { connectRedis = original }()

	conn := connectRedis()
	_, err = conn.Do("HSET", prefix+"M.corrupt"+detailSuffix, "content", `{not-json`)
	_ = conn.Close()
	if err != nil {
		t.Fatalf("seed corrupt article: %v", err)
	}
	if err := (Redis{}).FindE("M.corrupt", &Article{}); err == nil {
		t.Fatal("FindE() error = nil for corrupt JSON")
	}

	server.Set(prefix+"M.wrongtype"+detailSuffix, "not-a-hash")
	if err := (Redis{}).FindE("M.wrongtype", &Article{}); err == nil {
		t.Fatal("FindE() error = nil for wrong Redis type")
	}
}

func TestRedisDeleteRemovesDetailWithoutTouchingSubscribers(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	defer server.Close()
	original := connectRedis
	connectRedis = func() redis.Conn {
		conn, err := redis.Dial("tcp", server.Addr())
		if err != nil {
			t.Fatalf("dial miniredis: %v", err)
		}
		return conn
	}
	defer func() { connectRedis = original }()
	server.Set(prefix+"M.cached"+detailSuffix, "detail")
	conn := connectRedis()
	_, err = conn.Do("SADD", prefix+"M.cached"+subsSuffix, "discord-main")
	_ = conn.Close()
	if err != nil {
		t.Fatalf("seed subscriber: %v", err)
	}

	if err := (Redis{}).Delete("M.cached"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if server.Exists(prefix + "M.cached" + detailSuffix) {
		t.Fatal("article detail survived Delete")
	}
	member, err := server.IsMember(prefix+"M.cached"+subsSuffix, "discord-main")
	if err != nil || !member {
		t.Fatalf("subscriber membership = (%t, %v), want retained", member, err)
	}
}
