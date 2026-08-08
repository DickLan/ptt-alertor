package user

import (
	"encoding/json"
	"errors"

	log "github.com/Ptt-Alertor/logrus"

	"strings"

	"github.com/Ptt-Alertor/ptt-alertor/connections"
	"github.com/Ptt-Alertor/ptt-alertor/myutil"
	"github.com/garyburd/redigo/redis"
)

type Redis struct{}

var connectRedis = connections.Redis

const prefix string = "user:"

func (Redis) List() (accounts []string) {
	conn := connectRedis()
	defer conn.Close()
	userKeys, err := redis.Strings(conn.Do("KEYS", "user:*"))
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
	}
	for _, key := range userKeys {
		accounts = append(accounts, strings.TrimPrefix(key, "user:"))
	}
	return accounts
}

func (Redis) Exist(account string) bool {
	conn := connectRedis()
	defer conn.Close()
	key := prefix + account
	bl, err := redis.Bool(conn.Do("EXISTS", key))
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
	}
	return bl
}

func (Redis) Save(account string, data interface{}) error {
	conn := connectRedis()
	defer conn.Close()
	key := prefix + account
	uJSON, err := json.Marshal(data)
	if err != nil {
		myutil.LogJSONEncode(err, data)
		return err
	}

	_, err = redis.String(conn.Do("SET", key, uJSON, "NX"))
	if errors.Is(err, redis.ErrNil) {
		return ErrUserAlreadyExists
	}
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
		return err
	}
	return nil
}

func (Redis) Update(account string, user interface{}) error {
	conn := connectRedis()
	defer conn.Close()
	key := prefix + account
	uJSON, err := json.Marshal(user)
	if err != nil {
		myutil.LogJSONEncode(err, user)
		return err
	}

	_, err = redis.String(conn.Do("SET", key, uJSON, "XX"))
	if errors.Is(err, redis.ErrNil) {
		return ErrUserNotExist
	}
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
		return err
	}
	return nil
}

func (Redis) Find(account string, user *User) {
	if err := (Redis{}).FindE(account, user); err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
	}
}

// FindE distinguishes a missing user (zero value, nil error) from storage or
// JSON corruption. Reliable notification producers must not advance cursors
// after the latter failures.
func (Redis) FindE(account string, user *User) error {
	conn := connectRedis()
	defer conn.Close()

	key := prefix + account
	uJSON, err := redis.Bytes(conn.Do("GET", key))
	if errors.Is(err, redis.ErrNil) {
		return nil
	}
	if err != nil {
		return err
	}

	if uJSON != nil {
		err = json.Unmarshal(uJSON, user)
		if err != nil {
			myutil.LogJSONDecode(err, uJSON)
			return err
		}
	}
	return nil
}
