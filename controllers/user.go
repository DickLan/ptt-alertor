package controllers

import (
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"

	"github.com/Ptt-Alertor/ptt-alertor/models"
	"github.com/Ptt-Alertor/ptt-alertor/models/user"
	"github.com/Ptt-Alertor/ptt-alertor/myutil"
	"github.com/julienschmidt/httprouter"
)

func UserFind(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	u := models.User().Find(params.ByName("account"))
	if u.Profile.Account == "" {
		http.Error(w, "user not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	uJSON, err := json.Marshal(u)
	if err != nil {
		myutil.LogJSONEncode(err, u)
		http.Error(w, "failed to encode user", http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "%s", uJSON)
}

func UserAll(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	us := models.User().All()

	data := struct {
		Total, Discord, Line, Messenger, Telegram, IdleUser, BlockUser int
		SubCount, BoardCount, KeywordCount, AuthorCount, PushSumCount  int
		User, Room, Group                                              int
		Users                                                          []*user.User
	}{}
	data.Users = us
	data.Total = len(us)
	for _, u := range us {
		if !u.Enable {
			data.BlockUser++
		}
		if u.Profile.Discord {
			data.Discord++
		}
		if u.Profile.Line != "" {
			data.Line++
		}
		if u.Profile.Messenger != "" {
			data.Messenger++
		}
		if u.Profile.Telegram != "" {
			data.Telegram++
		}
		switch u.Profile.Type {
		case "user", "":
			data.User++
		case "room":
			data.Room++
		case "group":
			data.Group++
		}
		userSubCount := len(u.Subscribes)
		data.SubCount += userSubCount
		if userSubCount == 0 {
			data.IdleUser++
		}
		data.BoardCount += userSubCount
		for _, s := range u.Subscribes {
			data.KeywordCount += len(s.Keywords)
			data.AuthorCount += len(s.Authors)
			if s.PushSum.Up != 0 || s.PushSum.Down != 0 {
				data.PushSumCount++
			}
		}
	}
	t, err := template.ParseFiles("public/user.tpl")
	if err != nil {
		http.Error(w, "user template is unavailable", http.StatusInternalServerError)
		return
	}
	if err := t.Execute(w, data); err != nil {
		http.Error(w, "failed to render users", http.StatusInternalServerError)
	}
}

func UserCreate(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	u := models.User()
	if err := decodeUserRequest(w, r, u); err != nil {
		myutil.LogJSONDecode(err, r.Body)
		http.Error(w, "not a json valid format", http.StatusBadRequest)
		return
	}
	if len(u.Subscribes) != 0 {
		http.Error(w, "create the user with an empty subscribes list, then use /users/:account/commands", http.StatusBadRequest)
		return
	}
	commandMu.Lock()
	err := u.Save()
	commandMu.Unlock()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func UserModify(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	account := params.ByName("account")
	u := models.User()
	if err := decodeUserRequest(w, r, u); err != nil {
		myutil.LogJSONDecode(err, r.Body)
		http.Error(w, "not a json valid format", http.StatusBadRequest)
		return
	}

	if u.Profile.Account != account {
		http.Error(w, "account does not match", http.StatusBadRequest)
		return
	}
	commandMu.Lock()
	defer commandMu.Unlock()
	for attempt := 0; attempt < 3; attempt++ {
		existing := models.User().Find(account)
		if existing.Profile.Account == "" {
			http.Error(w, "user not found", http.StatusNotFound)
			return
		}
		// Subscription indexes are maintained by the command API; profile
		// updates preserve the latest indexed subscription list at commit time.
		u.Subscribes = existing.Clone().Subscribes
		u.CreateTime = existing.CreateTime
		err := u.UpdateIfUnchanged(existing)
		if errors.Is(err, user.ErrConcurrentUpdate) {
			continue
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	http.Error(w, "user changed concurrently; retry the request", http.StatusConflict)
}

func decodeUserRequest(w http.ResponseWriter, r *http.Request, target *user.User) error {
	return decodeStrictJSON(w, r, target)
}
