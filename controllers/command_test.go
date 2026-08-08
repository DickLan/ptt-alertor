package controllers

import (
	"net/http"
	"testing"

	appCommand "github.com/Ptt-Alertor/ptt-alertor/command"
)

func TestCommandHTTPStatusUsesStructuredKind(t *testing.T) {
	tests := []struct {
		name string
		kind appCommand.ExecutionKind
		want int
	}{
		{name: "success", kind: appCommand.ExecutionKindSuccess, want: http.StatusOK},
		{name: "invalid", kind: appCommand.ExecutionKindInvalid, want: http.StatusBadRequest},
		{name: "PTT unavailable", kind: appCommand.ExecutionKindPTTUnavailable, want: http.StatusServiceUnavailable},
		{name: "conflict", kind: appCommand.ExecutionKindConflict, want: http.StatusConflict},
		{name: "internal", kind: appCommand.ExecutionKindInternal, want: http.StatusInternalServerError},
		{name: "unknown fails closed", kind: appCommand.ExecutionKind("future-kind"), want: http.StatusInternalServerError},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := commandHTTPStatus(test.kind); got != test.want {
				t.Fatalf("commandHTTPStatus(%q) = %d, want %d", test.kind, got, test.want)
			}
		})
	}
}

func TestSuccessfulCommandTextMayContainFailureWord(t *testing.T) {
	result := appCommand.ExecutionResult{
		Message: "stock: 失敗",
		Kind:    appCommand.ExecutionKindSuccess,
	}
	if got := commandHTTPStatus(result.Kind); got != http.StatusOK {
		t.Fatalf("successful result %q status = %d, want %d", result.Message, got, http.StatusOK)
	}
}
