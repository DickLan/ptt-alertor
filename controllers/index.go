package controllers

import (
	"html/template"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/Ptt-Alertor/ptt-alertor/channels/discord"
	"github.com/Ptt-Alertor/ptt-alertor/models/counter"
	"github.com/Ptt-Alertor/ptt-alertor/models/top"
	"github.com/Ptt-Alertor/ptt-alertor/shorturl"
	"github.com/julienschmidt/httprouter"
)

var tpls = []string{
	"public/docs.html",
	"public/top.html",
	"public/discord.html",
	"public/tpls/head.tpl",
	"public/tpls/header.tpl",
	"public/tpls/slogan.tpl",
	"public/tpls/command.tpl",
	"public/tpls/counter.tpl",
	"public/tpls/footer.tpl",
	"public/tpls/script.tpl",
}

var (
	templatesOnce sync.Once
	templates     *template.Template
	templatesErr  error
	s3Domain      = os.Getenv("S3_DOMAIN")
)

func pageTemplates() (*template.Template, error) {
	templatesOnce.Do(func() {
		templates, templatesErr = template.ParseFiles(tpls...)
	})
	return templates, templatesErr
}

// Index Handles router "/" request
func Index(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	tpls, err := pageTemplates()
	if err != nil {
		http.Error(w, "page templates are unavailable", http.StatusInternalServerError)
		return
	}
	err = tpls.ExecuteTemplate(w, "discord.html", struct {
		URI               string
		Count             []string
		S3Domain          string
		DiscordConfigured bool
		JobsEnabled       bool
	}{
		URI:               "",
		Count:             count(),
		S3Domain:          s3Domain,
		DiscordConfigured: discord.NewFromEnv().Validate() == nil,
		JobsEnabled:       strings.EqualFold(strings.TrimSpace(os.Getenv("JOBS_ENABLED")), "true"),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func count() (counterStrs []string) {
	count, err := counter.Alert()
	if err != nil {
		return nil
	}
	countStrs := strings.Split((strconv.Itoa(count)), "")
	for index, num := range countStrs {
		counterStrs = append(counterStrs, num)
		if backIndex := len(countStrs) - index; backIndex != 1 && backIndex%3 == 1 {
			counterStrs = append(counterStrs, ",")
		}
	}
	return counterStrs
}

// Top Handles router "/top" request, it shows top rank of keywords, authors, pushsum
func Top(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	tpls, err := pageTemplates()
	if err != nil {
		http.Error(w, "page templates are unavailable", http.StatusInternalServerError)
		return
	}
	count := 100
	keywords := top.ListKeywordWithScore(count)
	authors := top.ListAuthorWithScore(count)
	pushsum := top.ListPushSumWithScore(count)
	data := struct {
		URI      string
		Keywords top.WordOrders
		Authors  top.WordOrders
		PushSum  top.WordOrders
		S3Domain string
	}{
		"top",
		keywords,
		authors,
		pushsum,
		s3Domain,
	}
	err = tpls.ExecuteTemplate(w, "top.html", data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// Docs shows advanced intructions
func Docs(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	tpls, err := pageTemplates()
	if err != nil {
		http.Error(w, "page templates are unavailable", http.StatusInternalServerError)
		return
	}
	err = tpls.ExecuteTemplate(w, "docs.html", struct {
		URI      string
		S3Domain string
	}{
		"docs",
		s3Domain,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// Redirect redirects short url to original url
func Redirect(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	checksum := params.ByName("checksum")
	url := shorturl.Original(checksum)
	if url != "" {
		http.Redirect(w, r, url, http.StatusMovedPermanently)
	} else {
		t, err := template.ParseFiles("public/404.html")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		t.Execute(w, nil)
	}
}
