package controllers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/julienschmidt/httprouter"
)

func TestUserCreateRejectsNullWithoutPanic(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/users", strings.NewReader("null"))
	UserCreate(recorder, request, nil)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}

func TestUserModifyRejectsNullWithoutPanic(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPut, "/users/discord-main", strings.NewReader("null"))
	UserModify(recorder, request, httprouter.Params{{Key: "account", Value: "discord-main"}})
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}

func TestUserCreateRejectsUnknownAndTrailingJSON(t *testing.T) {
	for _, body := range []string{
		`{"profile":{"account":"discord-main","discord":true},"unknown":true}`,
		`{"profile":{"account":"discord-main","discord":true}} {}`,
	} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/users", strings.NewReader(body))
		UserCreate(recorder, request, nil)
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("body %q status = %d, want %d", body, recorder.Code, http.StatusBadRequest)
		}
	}
}

func TestUserCreateRejectsAccountThatCannotBeUsedInRoute(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/users", strings.NewReader(
		`{"enable":true,"profile":{"account":"unreachable/account","discord":true},"subscribes":[]}`,
	))
	UserCreate(recorder, request, nil)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}
