package controllers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/julienschmidt/httprouter"
)

func TestCommandRejectsTrailingAndUnknownJSONBeforeExecution(t *testing.T) {
	for _, body := range []string{
		`{"command":"清單"} {}`,
		`{"command":"清單","unknown":true}`,
	} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/users/discord-main/commands", strings.NewReader(body))
		UserCommand(recorder, request, httprouter.Params{{Key: "account", Value: "discord-main"}})
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("body %q status = %d, want %d", body, recorder.Code, http.StatusBadRequest)
		}
	}
}

func TestBroadcastRejectsTrailingAndUnknownJSONBeforeDelivery(t *testing.T) {
	for _, body := range []string{
		`{"platforms":["discord"],"content":"test"} {}`,
		`{"platforms":["discord"],"content":"test","unknown":true}`,
	} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/broadcast", strings.NewReader(body))
		Broadcast(recorder, request, nil)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("body %q status = %d, want %d", body, recorder.Code, http.StatusBadRequest)
		}
	}
}

func TestStrictJSONRejectsOversizedBody(t *testing.T) {
	recorder := httptest.NewRecorder()
	body := `{"command":"` + strings.Repeat("x", int(maxJSONBodyBytes)) + `"}`
	request := httptest.NewRequest(http.MethodPost, "/users/discord-main/commands", strings.NewReader(body))
	UserCommand(recorder, request, httprouter.Params{{Key: "account", Value: "discord-main"}})
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}
