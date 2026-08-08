package controllers

import (
	"errors"
	"fmt"
	"net/http"

	log "github.com/Ptt-Alertor/logrus"
	"github.com/Ptt-Alertor/ptt-alertor/channels/discord"
	"github.com/Ptt-Alertor/ptt-alertor/jobs"
	"github.com/julienschmidt/httprouter"
)

func Broadcast(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	type requestBody struct {
		Platforms []string `json:"platforms"`
		Content   string   `json:"content"`
	}
	body := requestBody{}
	err := decodeStrictJSON(w, r, &body)
	if err != nil {
		log.WithError(err).Error("Decode Notify Body Failed")
		http.Error(w, "invalid JSON request body", http.StatusBadRequest)
		return
	}
	bc := new(jobs.Broadcaster)
	bc.Msg = body.Content
	err = bc.SendContext(r.Context(), body.Platforms)
	if err != nil {
		status := http.StatusBadGateway
		switch {
		case errors.Is(err, jobs.ErrNoBroadcastPlatform), errors.Is(err, jobs.ErrUnsupportedBroadcastPlatform), errors.Is(err, discord.ErrEmptyMessage), errors.Is(err, discord.ErrContentTooLong):
			status = http.StatusBadRequest
		case errors.Is(err, discord.ErrWebhookNotConfigured), errors.Is(err, discord.ErrInvalidWebhookURL):
			status = http.StatusServiceUnavailable
		}
		log.WithError(err).Error("Broadcast Failed")
		http.Error(w, err.Error(), status)
		return
	}
	fmt.Fprintln(w, "OK")
}
