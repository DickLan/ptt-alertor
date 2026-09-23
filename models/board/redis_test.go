package board

import (
	"net"
	"os"
	"testing"

	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	"github.com/alicebob/miniredis"
)

var boardRedis *miniredis.Miniredis

func TestMain(m *testing.M) {
	server, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	boardRedis = server
	host, port, err := net.SplitHostPort(server.Addr())
	if err != nil {
		panic(err)
	}
	_ = os.Setenv("REDIS_ENDPOINT", host)
	_ = os.Setenv("REDIS_PORT", port)
	code := m.Run()
	server.Close()
	os.Exit(code)
}

func TestRedisGetArticlesEDistinguishesMissingEmptyAndCorrupt(t *testing.T) {
	boardRedis.FlushAll()
	store := Redis{}

	articles, initialized, err := store.GetArticlesE("stock")
	if err != nil || initialized || articles != nil {
		t.Fatalf("missing = (%#v, %t, %v)", articles, initialized, err)
	}

	if err := store.Save("stock", article.Articles{}); err != nil {
		t.Fatal(err)
	}
	articles, initialized, err = store.GetArticlesE("stock")
	if err != nil || !initialized || len(articles) != 0 {
		t.Fatalf("empty snapshot = (%#v, %t, %v)", articles, initialized, err)
	}

	boardRedis.Set("board:stock", `{not-json`)
	if _, initialized, err = store.GetArticlesE("stock"); err == nil || !initialized {
		t.Fatalf("corrupt snapshot initialized=%t error=%v", initialized, err)
	}
}

func TestRedisCreateDoesNotAddBoardOutsidePollingAllowlist(t *testing.T) {
	t.Setenv(pollingAllowlistEnvironment, "HardwareSale,MacShop,PC_Shopping")
	boardRedis.FlushAll()
	store := Redis{}
	if err := store.Create("Stock"); err != nil {
		t.Fatalf("Create(disallowed): %v", err)
	}
	if store.Exist("Stock") {
		t.Fatal("disallowed board entered the polling set")
	}
	if err := store.Create("HardwareSale"); err != nil {
		t.Fatalf("Create(allowed): %v", err)
	}
	if !store.Exist("HardwareSale") {
		t.Fatal("allowed board did not enter the polling set")
	}
}

func TestRedisCreateRetainsHistoricalBehaviorWithEmptyAllowlist(t *testing.T) {
	t.Setenv(pollingAllowlistEnvironment, "")
	boardRedis.FlushAll()
	store := Redis{}
	if err := store.Create("Stock"); err != nil {
		t.Fatalf("Create(): %v", err)
	}
	if !store.Exist("Stock") {
		t.Fatal("empty allowlist prevented the historical unrestricted create")
	}
}
