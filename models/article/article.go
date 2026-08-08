package article

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	log "github.com/Ptt-Alertor/logrus"
	"github.com/Ptt-Alertor/ptt-alertor/connections"
	"github.com/Ptt-Alertor/ptt-alertor/models/pushsum"
	"github.com/Ptt-Alertor/ptt-alertor/myutil"
	"github.com/garyburd/redigo/redis"
)

const prefix = "article:"
const subsSuffix = ":subs"

var (
	// PTT article filenames use the full code as their stable identity. The
	// leading timestamp alone is not unique: two articles can be created during
	// the same second and differ only in the final hash.
	articleCodePattern = regexp.MustCompile(`(?:^|/)([GM]\.(\d+)\.A\.[[:alnum:]_-]+)(?:\.html)?(?:[?#].*)?$`)
	// Keep ParseID compatible with older/non-standard PTT links even when they do
	// not have the modern .A.<hash> suffix. ID remains metadata, not identity.
	articleIDPattern = regexp.MustCompile(`(?:^|/)[GM]\.(\d+)\.`)
)

type Article struct {
	ID               int    `json:"ID,omitempty"`
	Code             string `json:"code,omitempty"`
	Title            string
	Link             string
	Date             string    `json:"Date,omitempty"`
	Author           string    `json:"Author,omitempty"`
	Comments         Comments  `json:"comments,omitempty"`
	LastPushDateTime time.Time `json:"lastPushDateTime,omitempty"`
	Board            string    `json:"board,omitempty"`
	PushSum          int       `json:"pushSum,omitempty"`
	drive            Driver
}

type Driver interface {
	Find(code string, article *Article)
	Save(a Article) error
	Delete(code string) error
}

type errorFindingDriver interface {
	FindE(code string, article *Article) error
}

func NewArticle(drive Driver) *Article {
	return &Article{
		drive: drive,
	}
}

// ParseCode extracts the complete PTT article filename without the .html
// suffix. Both modern M.* and legacy G.* article codes are supported.
func (a Article) ParseCode(link string) string {
	matches := articleCodePattern.FindStringSubmatch(strings.TrimSpace(link))
	if len(matches) < 2 {
		return ""
	}
	return matches[1]
}

// Identity returns the stable identity used to compare articles. Code is
// preferred, including a code derived from a legacy cached Link. A non-PTT
// link remains a usable fallback. ID is deliberately not used because it only
// has one-second precision.
func (a Article) Identity() string {
	if code := strings.TrimSpace(a.Code); code != "" {
		return code
	}
	if code := a.ParseCode(a.Link); code != "" {
		return code
	}
	return strings.TrimSpace(a.Link)
}

func (a Article) ParseID(link string) (id int) {
	strs := articleIDPattern.FindStringSubmatch(strings.TrimSpace(link))
	if len(strs) < 2 {
		return 0
	}
	id, err := strconv.Atoi(strs[1])
	if err != nil {
		return 0
	}
	return id
}

func (a Article) MatchKeyword(keyword string) bool {
	// v1 reserves author: only when it begins an AND expression. Keeping the
	// prefix in this single position avoids reinterpreting established title
	// rules such as "sale&author:name" or the standalone "author:name".
	if strings.HasPrefix(keyword, "author:") && strings.Contains(keyword, "&") {
		terms := strings.Split(keyword, "&")
		author := strings.TrimSpace(strings.TrimPrefix(terms[0], "author:"))
		if author == "" {
			return false
		}
		for _, term := range terms[1:] {
			if strings.TrimSpace(term) == "" || !matchKeyword(a.Title, term) {
				return false
			}
		}
		return a.MatchAuthor(author)
	}
	if strings.Contains(keyword, "&") {
		keywords := strings.Split(keyword, "&")
		for _, keyword := range keywords {
			if !matchKeyword(a.Title, keyword) {
				return false
			}
		}
		return true
	}
	if strings.HasPrefix(keyword, "regexp:") {
		return matchRegex(a.Title, keyword)
	}
	return matchKeyword(a.Title, keyword)
}

// MatchAuthor reports whether the displayed PTT author contains the subscribed
// value, ignoring case. An empty subscription never matches: treating it as a
// substring would accidentally turn a malformed author rule into an all-posts
// subscription.
func (a Article) MatchAuthor(author string) bool {
	author = strings.TrimSpace(author)
	if author == "" {
		return false
	}
	return strings.Contains(strings.ToLower(a.Author), strings.ToLower(author))
}

// Exist check article exist or not
func (a Article) Exist() (bool, error) {
	conn := connections.Redis()
	defer conn.Close()

	bl, err := redis.Bool(conn.Do("EXISTS", prefix+a.Code+subsSuffix, "board"))
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
	}
	return bl, err
}

func (a Article) Find(code string) Article {
	found, err := a.FindE(code)
	if err != nil {
		log.WithField("code", code).WithError(err).Error("Find Article Failed")
	}
	return found
}

// FindE distinguishes a missing tracked article from a Redis/JSON failure.
// Reliable comment producers must retain their cursor after the latter.
func (a Article) FindE(code string) (Article, error) {
	if finder, ok := a.drive.(errorFindingDriver); ok {
		if err := finder.FindE(code, &a); err != nil {
			return a, err
		}
		return a, nil
	}
	a.drive.Find(code, &a)
	return a, nil
}

func (a Article) Save() error {
	return a.drive.Save(a)
}

func (a Article) Destroy() error {
	if err := a.drive.Delete(a.Code); err != nil {
		return err
	}

	conn := connections.Redis()
	defer conn.Close()

	_, err := conn.Do("DEL", prefix+a.Code+subsSuffix)
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
	}
	return err
}

func (a Article) AddSubscriber(account string) error {
	conn := connections.Redis()
	defer conn.Close()

	_, err := conn.Do("SADD", prefix+a.Code+subsSuffix, account)
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
	}
	return err
}

func (a Article) Subscribers() ([]string, error) {
	conn := connections.Redis()
	defer conn.Close()

	accounts, err := redis.Strings(conn.Do("SMEMBERS", prefix+a.Code+subsSuffix))
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
	}
	return accounts, err
}

func (a Article) RemoveSubscriber(sub string) error {
	conn := connections.Redis()
	defer conn.Close()

	_, err := conn.Do("SREM", prefix+a.Code+subsSuffix, sub)
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
	}
	return err
}

func (a Article) String() string {
	return a.Title + "\r\n" + a.Link
}

func (a Article) StringWithPushSum() string {
	sumStr := strconv.Itoa(a.PushSum)
	if text, ok := pushsum.NumTextMap[a.PushSum]; ok {
		sumStr = text
	}
	return fmt.Sprintf("%s %s\r\n%s", sumStr, a.Title, a.Link)
}

func matchRegex(title string, regex string) bool {
	pattern := strings.TrimPrefix(regex, "regexp:")
	b, err := regexp.MatchString(pattern, title)
	if err != nil {
		return false
	}
	return b
}

func matchKeyword(title string, keyword string) bool {
	if strings.HasPrefix(keyword, "!") {
		excludeKeyword := strings.Trim(keyword, "!")
		return !containKeyword(title, excludeKeyword)
	}
	return containKeyword(title, keyword)
}

func containKeyword(title string, keyword string) bool {
	return strings.Contains(strings.ToLower(title), strings.ToLower(keyword))
}
