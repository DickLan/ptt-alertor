package controllers

import (
	"net/http/httptest"
	"testing"
)

func TestSameWebSocketOrigin(t *testing.T) {
	tests := []struct {
		name   string
		host   string
		origin string
		want   bool
	}{
		{name: "same origin", host: "alerts.example:9090", origin: "https://alerts.example:9090", want: true},
		{name: "case insensitive host", host: "ALERTS.EXAMPLE", origin: "https://alerts.example", want: true},
		{name: "different host", host: "alerts.example", origin: "https://evil.example", want: false},
		{name: "missing origin", host: "alerts.example", want: false},
		{name: "invalid origin", host: "alerts.example", origin: "://", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "http://"+test.host+"/ws", nil)
			r.Host = test.host
			if test.origin != "" {
				r.Header.Set("Origin", test.origin)
			}
			if got := sameWebSocketOrigin(r); got != test.want {
				t.Fatalf("sameWebSocketOrigin() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestCounterHubCapsClientsAndKeepsLatestValue(t *testing.T) {
	hub := newCounterHub(1)
	updates, ok := hub.add()
	if !ok {
		t.Fatal("first client was rejected")
	}
	if _, ok := hub.add(); ok {
		t.Fatal("client beyond cap was accepted")
	}

	hub.broadcast([]byte("1"))
	hub.broadcast([]byte("2"))
	if got := string(<-updates); got != "2" {
		t.Fatalf("latest update = %q, want 2", got)
	}

	hub.remove(updates)
	if _, ok := hub.add(); !ok {
		t.Fatal("slot was not released after client removal")
	}
}
