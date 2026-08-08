package counter

import (
	"net"
	"testing"

	"github.com/alicebob/miniredis"
)

func TestAlertTreatsMissingCounterAsZero(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	host, port, err := net.SplitHostPort(server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("REDIS_ENDPOINT", host)
	t.Setenv("REDIS_PORT", port)

	count, err := Alert()
	if err != nil || count != 0 {
		t.Fatalf("Alert() = (%d, %v), want (0, nil)", count, err)
	}
}
