package controllers

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"

	appCommand "github.com/Ptt-Alertor/ptt-alertor/command"
	"github.com/Ptt-Alertor/ptt-alertor/models"
	"github.com/julienschmidt/httprouter"
)

var commandMu sync.Mutex

// UserCommand applies the same subscription commands that the legacy chat
// integrations used, while Discord remains an outbound-only webhook.
func UserCommand(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	account := strings.TrimSpace(params.ByName("account"))
	if account == "" {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}

	var body struct {
		Command string `json:"command"`
	}
	if err := decodeStrictJSON(w, r, &body); err != nil {
		http.Error(w, "invalid JSON request body", http.StatusBadRequest)
		return
	}
	body.Command = strings.TrimSpace(body.Command)
	if body.Command == "" {
		http.Error(w, "command is required", http.StatusBadRequest)
		return
	}

	ctx, cancel := pttOperationContext(r.Context())
	defer cancel()
	// User records are stored as whole JSON documents. Serialize commands in
	// this process to reduce optimistic-transaction conflicts. Redis WATCH and
	// MULTI remain the correctness boundary for concurrent replicas.
	commandMu.Lock()
	user := models.User().Find(account)
	if user.Profile.Account == "" {
		commandMu.Unlock()
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}
	result := appCommand.ExecuteCommandContext(ctx, body.Command, account, true)
	commandMu.Unlock()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(commandHTTPStatus(result.Kind))
	_ = json.NewEncoder(w).Encode(map[string]string{"result": result.Message})
}

func commandHTTPStatus(kind appCommand.ExecutionKind) int {
	switch kind {
	case appCommand.ExecutionKindSuccess:
		return http.StatusOK
	case appCommand.ExecutionKindInvalid:
		return http.StatusBadRequest
	case appCommand.ExecutionKindPTTUnavailable:
		return http.StatusServiceUnavailable
	case appCommand.ExecutionKindConflict:
		return http.StatusConflict
	case appCommand.ExecutionKindInternal:
		return http.StatusInternalServerError
	default:
		// New result kinds must be explicitly mapped before they can be exposed
		// as successful HTTP responses.
		return http.StatusInternalServerError
	}
}
