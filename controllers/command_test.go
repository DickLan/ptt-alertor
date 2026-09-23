package controllers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	appCommand "github.com/Ptt-Alertor/ptt-alertor/command"
	"github.com/Ptt-Alertor/ptt-alertor/models"
	"github.com/Ptt-Alertor/ptt-alertor/models/user"
	"github.com/julienschmidt/httprouter"
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

func TestUserCommandReturnsBadRequestForBoardOutsideAllowlist(t *testing.T) {
	t.Setenv("BOARD_ALLOWLIST", "HardwareSale,MacShop,PC_Shopping")
	controllerRedis.FlushAll()
	u := models.User()
	u.Enable = true
	u.Profile = user.Profile{Account: "discord-main", Discord: true}
	if err := u.Save(); err != nil {
		t.Fatalf("save user: %v", err)
	}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		"/users/discord-main/commands",
		strings.NewReader(`{"command":"新增 Stock tsmc"}`),
	)
	UserCommand(recorder, request, httprouter.Params{{Key: "account", Value: "discord-main"}})
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%q", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["result"] != "此看板目前不接受新增訂閱。" {
		t.Fatalf("result = %q, want generic allowlist rejection", body["result"])
	}
	if strings.Contains(recorder.Body.String(), "HardwareSale") || strings.Contains(recorder.Body.String(), "BOARD_ALLOWLIST") {
		t.Fatalf("response leaked allowlist configuration: %q", recorder.Body.String())
	}
	if stored := models.User().Find("discord-main"); len(stored.Subscribes) != 0 {
		t.Fatalf("rejected API command stored subscriptions: %#v", stored.Subscribes)
	}
}
