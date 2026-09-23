// Package stockwatch owns the stock website's independent notification list.
package stockwatch

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/Ptt-Alertor/ptt-alertor/connections"
	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	"github.com/garyburd/redigo/redis"
)

const Prefix = "ptta:stock-quant"
const IntervalSeconds = int64(2 * 60 * 60)
const MaxAuthors = 100

var ErrStale = errors.New("stock watch attempt is stale")
var ErrInvalidAuthor = errors.New("invalid complete PTT author ID")
var ErrInvalidArticleScope = errors.New("invalid article scope")
var ErrAuthorNotFound = errors.New("notification author not found")
var ErrAuthorLimit = errors.New("author limit exceeded")
var authorPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_]{0,31}$`)

type Author struct {
	Author       string `json:"author"`
	Key          string `json:"key"`
	Generation   string `json:"generation"`
	ActivatedAt  int64  `json:"activated_at"`
	ArticleScope string `json:"article_scope"`
}

type Attempt struct {
	Token     string
	Epoch     string
	StartedMS int64
}

type Store struct {
	Connect func() redis.Conn
	Prefix  string
}

func NewStore() *Store                    { return &Store{Connect: connections.Redis, Prefix: Prefix} }
func (s *Store) key(suffix string) string { return s.Prefix + ":" + suffix }

func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func normalizeAuthor(value string) (string, string, error) {
	value = strings.TrimSpace(value)
	if !authorPattern.MatchString(value) {
		return "", "", ErrInvalidAuthor
	}
	return value, strings.ToLower(value), nil
}

const configTypes = `
local function expect(key,want)
 local t=redis.call('TYPE',key); if type(t)=='table' then t=t.ok end
 if t~='none' and t~=want then return false end
 return true
end
if not expect(KEYS[1],'hash') or not expect(KEYS[2],'hash') or not expect(KEYS[3],'string') or not expect(KEYS[4],'string') then return redis.error_reply('invalid watch storage types') end
`

const redisNow = `local t=redis.call('TIME'); local now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000); `

func (s *Store) Add(names []string) error {
	if len(names) == 0 || len(names) > MaxAuthors {
		return ErrInvalidAuthor
	}
	args := []interface{}{s.key("authors"), s.key("state"), s.key("cursor"), s.key("lock")}
	epoch, err := randomID()
	if err != nil {
		return err
	}
	args = append(args, epoch, IntervalSeconds*1000, MaxAuthors)
	seen := map[string]bool{}
	for _, name := range names {
		display, key, err := normalizeAuthor(name)
		if err != nil {
			return err
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		generation, err := randomID()
		if err != nil {
			return err
		}
		args = append(args, key, display, generation)
	}
	c := s.Connect()
	defer c.Close()
	result, err := redis.Int(redis.NewScript(4, configTypes+redisNow+`
local count=redis.call('HLEN',KEYS[1]); local added=0
for i=4,#ARGV,3 do if redis.call('HEXISTS',KEYS[1],ARGV[i])==0 then added=added+1 end end
if count+added>tonumber(ARGV[3]) then return -1 end
if count==0 and added>0 then
 redis.call('DEL',KEYS[3],KEYS[4])
 redis.call('HSET',KEYS[2],'epoch',ARGV[1],'next_attempt_at_ms',now+tonumber(ARGV[2]),'last_error','','consecutive_failures','0','cursor_from_ms',math.floor(now/1000)*1000)
end
for i=4,#ARGV,3 do
 local value=cjson.encode({key=ARGV[i],author=ARGV[i+1],generation=ARGV[i+2],activated_at=math.floor(now/1000),article_scope='all'})
 redis.call('HSETNX',KEYS[1],ARGV[i],value)
end
return added`).Do(c, args...))
	if err == nil && result == -1 {
		return ErrAuthorLimit
	}
	return err
}

// SetArticleScope changes only an existing author's filter. It does not reset
// activation, generation, the shared cursor or the next scheduled check.
func (s *Store) SetArticleScope(name, scope string) error {
	_, key, err := normalizeAuthor(name)
	if err != nil {
		return err
	}
	if scope != "all" && scope != "targets" {
		return ErrInvalidArticleScope
	}
	c := s.Connect()
	defer c.Close()
	result, err := redis.Int(redis.NewScript(1, `
local t=redis.call('TYPE',KEYS[1]); if type(t)=='table' then t=t.ok end
if t~='none' and t~='hash' then return redis.error_reply('invalid watch storage type') end
local raw=redis.call('HGET',KEYS[1],ARGV[1])
if not raw then return -1 end
local ok,a=pcall(cjson.decode,raw)
if not ok or type(a)~='table' or a.key~=ARGV[1]
 or type(a.author)~='string' or #a.author>32 or not string.match(a.author,'^[A-Za-z0-9][A-Za-z0-9_]*$') or string.lower(a.author)~=a.key
 or type(a.generation)~='string' or #a.generation~=32
 or type(a.activated_at)~='number' or a.activated_at<=0 or a.activated_at~=math.floor(a.activated_at)
 then return redis.error_reply('invalid notification author record') end
local scope=a.article_scope
if scope==nil or scope=='' then scope='all' end
if scope==ARGV[2] then return 0 end
redis.call('HSET',KEYS[1],ARGV[1],cjson.encode({key=a.key,author=a.author,generation=a.generation,activated_at=a.activated_at,article_scope=ARGV[2]}))
return 1`).Do(c, s.key("authors"), key, scope))
	if err != nil {
		return err
	}
	if result == -1 {
		return ErrAuthorNotFound
	}
	return nil
}

func (s *Store) Remove(name string) error {
	_, key, err := normalizeAuthor(name)
	if err != nil {
		return err
	}
	epoch, err := randomID()
	if err != nil {
		return err
	}
	c := s.Connect()
	defer c.Close()
	_, err = redis.NewScript(4, configTypes+`
local removed=redis.call('HDEL',KEYS[1],ARGV[1])
if redis.call('HLEN',KEYS[1])==0 then
 redis.call('DEL',KEYS[3],KEYS[4])
 redis.call('HSET',KEYS[2],'epoch',ARGV[2],'next_attempt_at_ms','0','last_error','','consecutive_failures','0','cursor_from_ms','0')
end
return removed`).Do(c, s.key("authors"), s.key("state"), s.key("cursor"), s.key("lock"), key, epoch)
	return err
}

func (s *Store) Authors() ([]Author, error) {
	c := s.Connect()
	defer c.Close()
	values, err := redis.StringMap(c.Do("HGETALL", s.key("authors")))
	if err != nil {
		return nil, err
	}
	result := make([]Author, 0, len(values))
	for key, raw := range values {
		var a Author
		if json.Unmarshal([]byte(raw), &a) != nil || a.Key != key || a.Key != strings.ToLower(a.Author) || a.ActivatedAt <= 0 || len(a.Generation) != 32 || !authorPattern.MatchString(a.Author) {
			return nil, errors.New("invalid notification author record")
		}
		if a.ArticleScope == "" {
			a.ArticleScope = "all"
		}
		result = append(result, a)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result, nil
}

func (s *Store) State() (map[string]string, error) {
	c := s.Connect()
	defer c.Close()
	return redis.StringMap(c.Do("HGETALL", s.key("state")))
}

func (s *Store) Begin() (*Attempt, error) {
	token, err := randomID()
	if err != nil {
		return nil, err
	}
	c := s.Connect()
	defer c.Close()
	reply, err := redis.Values(redis.NewScript(3, redisNow+`
if redis.call('HLEN',KEYS[1])==0 then return {} end
local due=tonumber(redis.call('HGET',KEYS[2],'next_attempt_at_ms'))
local epoch=redis.call('HGET',KEYS[2],'epoch')
if not due or not epoch then return redis.error_reply('invalid watch schedule') end
if now<due then return {} end
if not redis.call('SET',KEYS[3],ARGV[1],'NX','PX',ARGV[2]) then return {} end
redis.call('HSET',KEYS[2],'next_attempt_at_ms',now+tonumber(ARGV[2]),'last_attempt_at_ms',now)
return {epoch,now}`).Do(c, s.key("authors"), s.key("state"), s.key("lock"), token, IntervalSeconds*1000))
	if err != nil {
		return nil, err
	}
	if len(reply) == 0 {
		return nil, nil
	}
	a := &Attempt{Token: token}
	_, err = redis.Scan(reply, &a.Epoch, &a.StartedMS)
	return a, err
}

const fenceLua = `if redis.call('GET',KEYS[1])~=ARGV[1] or redis.call('HGET',KEYS[2],'epoch')~=ARGV[2] then return redis.error_reply('stale watch attempt') end; `

func (s *Store) Fence(a *Attempt) error {
	c := s.Connect()
	defer c.Close()
	_, err := redis.NewScript(2, fenceLua+`return 1`).Do(c, s.key("lock"), s.key("state"), a.Token, a.Epoch)
	if err != nil {
		return fmt.Errorf("%w: attempt fence unavailable", ErrStale)
	}
	return nil
}

func (s *Store) Release(a *Attempt) {
	c := s.Connect()
	defer c.Close()
	_, _ = redis.NewScript(1, `if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0`).Do(c, s.key("lock"), a.Token)
}

func (s *Store) Finish(a *Attempt, online article.Articles, failure string, scanned, matched int) error {
	encoded, err := json.Marshal(online)
	if err != nil {
		return err
	}
	var high int64
	for _, item := range online {
		if ts, ok := codeTimestamp(item); ok && ts > high {
			high = ts
		}
	}
	c := s.Connect()
	defer c.Close()
	_, err = redis.NewScript(3, fenceLua+redisNow+`
redis.call('HSET',KEYS[2],'last_error',ARGV[3],'last_scanned',ARGV[5],'last_matched',ARGV[6])
if ARGV[3]~='' then redis.call('HINCRBY',KEYS[2],'consecutive_failures',1); return 0 end
redis.call('SET',KEYS[3],ARGV[4])
redis.call('HSET',KEYS[2],'last_success_at_ms',now,'consecutive_failures','0','cursor_from_ms',ARGV[7])
return 1`).Do(c, s.key("lock"), s.key("state"), s.key("cursor"), a.Token, a.Epoch, failure, encoded, scanned, matched, high*1000)
	return err
}

type cursorDriver struct {
	store    *Store
	attempt  *Attempt
	boundary int64
}

func (d cursorDriver) GetArticles(name string) article.Articles {
	rows, _, _ := d.GetArticlesE(name)
	return rows
}
func (d cursorDriver) GetArticlesE(_ string) (article.Articles, bool, error) {
	c := d.store.Connect()
	defer c.Close()
	raw, err := redis.Bytes(redis.NewScript(3, fenceLua+`return redis.call('GET',KEYS[3])`).Do(c, d.store.key("lock"), d.store.key("state"), d.store.key("cursor"), d.attempt.Token, d.attempt.Epoch))
	if err == redis.ErrNil {
		return article.Articles{{ID: int(d.boundary - 1)}}, true, nil
	}
	if err != nil {
		return nil, false, err
	}
	var rows article.Articles
	if err = json.Unmarshal(raw, &rows); err != nil {
		return nil, true, err
	}
	if len(rows) == 0 {
		return nil, true, errors.New("empty stored watch cursor")
	}
	return rows, true, nil
}
func (d cursorDriver) Save(_ string, _ article.Articles) error {
	return errors.New("watch cursor requires a fenced commit")
}
func (d cursorDriver) Delete(_ string) error {
	return errors.New("watch cursor requires an epoch reset")
}

func (s *Store) MarkMalformed(a *Attempt, identity string) (bool, error) {
	c := s.Connect()
	defer c.Close()
	count, err := redis.Int(redis.NewScript(3, fenceLua+`
if redis.call('HLEN',KEYS[3])>=1000 and redis.call('HEXISTS',KEYS[3],ARGV[3])==0 then return redis.error_reply('malformed item limit') end
local count=redis.call('HINCRBY',KEYS[3],ARGV[3],1)
if count==3 then redis.call('HINCRBY',KEYS[2],'skipped_invalid',1) end
return count`).Do(c, s.key("lock"), s.key("state"), s.key("invalid"), a.Token, a.Epoch, identity))
	return count >= 3, err
}

func (s *Store) Recent() ([]map[string]string, error) {
	c := s.Connect()
	defer c.Close()
	rows, err := redis.Strings(c.Do("LRANGE", s.key("history"), 0, 19))
	if err != nil {
		return nil, err
	}
	result := make([]map[string]string, 0, len(rows))
	for _, raw := range rows {
		var r map[string]string
		if err = json.Unmarshal([]byte(raw), &r); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, nil
}

func (s *Store) RecordDelivery(itemKind, messageID string) error {
	parts := strings.Split(itemKind, ":")
	if len(parts) != 4 {
		return errors.New("invalid watch item kind")
	}
	c := s.Connect()
	defer c.Close()
	_, err := redis.NewScript(2, redisNow+`
local value=cjson.encode({author=ARGV[1],code=ARGV[2],message_id=ARGV[3],sent_at_ms=tostring(now)})
redis.call('LPUSH',KEYS[1],value); redis.call('LTRIM',KEYS[1],0,19)
redis.call('HSET',KEYS[2],'last_sent_at_ms',now,'delivery_error','')
return 1`).Do(c, s.key("history"), s.key("state"), parts[1], parts[3], messageID)
	return err
}

func (s *Store) DeliveryFailure(code string) {
	c := s.Connect()
	defer c.Close()
	_, _ = c.Do("HSET", s.key("state"), "delivery_error", code)
}
func number(value string) int64 { n, _ := strconv.ParseInt(value, 10, 64); return n }
