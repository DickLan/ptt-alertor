package board

import (
	"encoding/json"

	log "github.com/Ptt-Alertor/logrus"
	"github.com/garyburd/redigo/redis"

	"github.com/Ptt-Alertor/ptt-alertor/connections"
	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	"github.com/Ptt-Alertor/ptt-alertor/myutil"
)

const prefix string = "board:"

type Redis struct {
}

func (Redis) List() []string {
	conn := connections.Redis()
	defer conn.Close()
	boards, err := redis.Strings(conn.Do("SMEMBERS", "boards"))
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
	}
	return boards
}

func (Redis) Exist(boardName string) bool {
	conn := connections.Redis()
	defer conn.Close()
	bl, err := redis.Bool(conn.Do("SISMEMBER", "boards", boardName))
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
	}
	return bl
}

func (Redis) Create(boardName string) error {
	conn := connections.Redis()
	defer conn.Close()
	_, err := conn.Do("SADD", "boards", boardName)
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
	}
	return err
}

func (Redis) Remove(boardName string) error {
	conn := connections.Redis()
	defer conn.Close()
	if _, err := conn.Do("SREM", "boards", boardName); err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
		return err
	}
	return nil
}

func (Redis) GetArticles(boardName string) (articles article.Articles) {
	articles, _, err := (Redis{}).GetArticlesE(boardName)
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
	}
	return articles
}

// GetArticlesE distinguishes an uninitialized board cursor from a valid empty
// snapshot and from Redis/JSON failure.
func (Redis) GetArticlesE(boardName string) (articles article.Articles, initialized bool, err error) {
	conn := connections.Redis()
	defer conn.Close()

	key := prefix + boardName
	articlesJSON, err := redis.Bytes(conn.Do("GET", key))
	if err == redis.ErrNil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	if err = json.Unmarshal(articlesJSON, &articles); err != nil {
		myutil.LogJSONDecode(err, articlesJSON)
		return nil, true, err
	}
	return articles, true, nil
}

func (Redis) Save(boardName string, articles article.Articles) error {
	conn := connections.Redis()
	defer conn.Close()

	articlesJSON, err := json.Marshal(articles)
	if err != nil {
		myutil.LogJSONEncode(err, articles)
		return err
	}
	_, err = conn.Do("SET", prefix+boardName, articlesJSON)
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
	}
	return err
}

func (Redis) Delete(boardName string) error {
	conn := connections.Redis()
	defer conn.Close()
	if _, err := conn.Do("DEL", prefix+boardName); err != nil {
		return err
	}
	return nil
}
