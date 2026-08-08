package pushsum

import (
	"context"
	"net"
	"os"
	"reflect"
	"testing"

	"github.com/alicebob/miniredis"
)

var pushSumRedis *miniredis.Miniredis

func TestMain(m *testing.M) {
	server, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	pushSumRedis = server
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

func TestThresholdRevisionSeedsIndependentlyThenReportsCrossing(t *testing.T) {
	pushSumRedis.FlushAll()

	first, err := PendingDiff("account", "stock", "up", "50", "article-a")
	if err != nil {
		t.Fatal(err)
	}
	if first.Initialized || len(first.New) != 0 {
		t.Fatalf("first state = %#v, want uninitialized baseline", first)
	}
	if err := CommitDiff("account", "stock", "up", "50", "article-a"); err != nil {
		t.Fatal(err)
	}
	second, err := PendingDiff("account", "stock", "up", "50", "article-a", "article-b")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(second.New, []string{"article-b"}) {
		t.Fatalf("50 revision new = %#v", second.New)
	}

	// Raising the threshold creates a distinct baseline. An article currently
	// below 100 is not recorded; when it later crosses 100 it is reported even
	// though it was already known to the 50 revision.
	higher, err := PendingDiff("account", "stock", "up", "100")
	if err != nil {
		t.Fatal(err)
	}
	if higher.Initialized {
		t.Fatal("100 revision unexpectedly initialized")
	}
	if err := CommitDiff("account", "stock", "up", "100"); err != nil {
		t.Fatal(err)
	}
	crossed, err := PendingDiff("account", "stock", "up", "100", "article-b")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(crossed.New, []string{"article-b"}) {
		t.Fatalf("100 revision crossing = %#v", crossed.New)
	}
}

func TestRotationKeepsInitializedMarkerAndDoesNotSwallowCrossing(t *testing.T) {
	pushSumRedis.FlushAll()
	if err := CommitDiff("account", "stock", "up", "50", "article-a"); err != nil {
		t.Fatal(err)
	}
	if err := ReplaceBenchKeysContext(context.Background()); err != nil {
		t.Fatal(err)
	}

	state, err := PendingDiff("account", "stock", "up", "50", "article-a", "article-b")
	if err != nil {
		t.Fatal(err)
	}
	if !state.Initialized {
		t.Fatal("rotation lost initialized marker")
	}
	if !reflect.DeepEqual(state.New, []string{"article-b"}) {
		t.Fatalf("post-rotation new = %#v, want article-b", state.New)
	}
}

func TestPendingDiffDeduplicatesCurrentIdentities(t *testing.T) {
	pushSumRedis.FlushAll()
	if err := CommitDiff("account", "stock", "down", "10"); err != nil {
		t.Fatal(err)
	}
	state, err := PendingDiff("account", "stock", "down", "10", "x", "x", "", " y ")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(state.New, []string{"x", "y"}) {
		t.Fatalf("new = %#v", state.New)
	}
}
