package pushsum

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	log "github.com/Ptt-Alertor/logrus"
	"github.com/Ptt-Alertor/ptt-alertor/connections"
	"github.com/Ptt-Alertor/ptt-alertor/myutil"
	"github.com/garyburd/redigo/redis"
)

const prefix string = "pushsum:"

var NumTextMap = map[int]string{
	100:  "爆",
	-10:  "X1",
	-20:  "X2",
	-30:  "X3",
	-40:  "X4",
	-50:  "X5",
	-60:  "X6",
	-70:  "X7",
	-80:  "X8",
	-90:  "X9",
	-100: "XX",
}

func ConvertPushCount(str string) int {
	for num, text := range NumTextMap {
		if strings.EqualFold(str, text) {
			return num
		}
	}
	cnt, err := strconv.Atoi(str)
	if err != nil {
		cnt = 0
	}
	return cnt
}

func List() []string {
	conn := connections.Redis()
	defer conn.Close()
	boards, err := redis.Strings(conn.Do("SMEMBERS", prefix+"boards"))
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
	}
	return boards
}

func Exist(board string) bool {
	conn := connections.Redis()
	defer conn.Close()
	bl, err := redis.Bool(conn.Do("SISMEMBER", prefix+"boards", board))
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
	}
	return bl
}

func Add(board string) error {
	conn := connections.Redis()
	defer conn.Close()
	_, err := conn.Do("SADD", prefix+"boards", board)
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
	}
	return err
}

func Remove(board string) error {
	conn := connections.Redis()
	defer conn.Close()
	_, err := conn.Do("SREM", prefix+"boards", board)
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
	}
	return err
}

func AddSubscriber(board, account string) error {
	conn := connections.Redis()
	defer conn.Close()
	_, err := conn.Do("SADD", prefix+board+":subs", account)
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
	}
	return err
}

func RemoveSubscriber(board, account string) error {
	conn := connections.Redis()
	defer conn.Close()
	_, err := conn.Do("SREM", prefix+board+":subs", account)
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
	}
	return err
}

func ListSubscribers(board string) []string {
	subs, err := ListSubscribersE(board)
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
	}
	return subs
}

// ListSubscribersE distinguishes no subscribers from a Redis failure.
func ListSubscribersE(board string) ([]string, error) {
	conn := connections.Redis()
	defer conn.Close()
	subs, err := redis.Strings(conn.Do("SMEMBERS", prefix+board+":subs"))
	return subs, err
}

func Destroy(board string) error {
	key := prefix + board + ":subs"
	conn := connections.Redis()
	defer conn.Close()
	_, err := conn.Do("DEL", key)
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
	}
	return err
}

// DiffState describes which current identities have not been committed for a
// particular threshold revision. Initialized is false only for the first
// baseline; it remains true across the 48-hour base/bench rotation.
type DiffState struct {
	Initialized bool
	New         []string
}

func diffKeyPrefix(account, board, kind, revision string) string {
	return prefix + account + ":" + strings.ToLower(board) + ":" + kind + ":" + revision + ":"
}

// PendingDiff reads notification state without advancing it. A caller can
// durably enqueue every item in New and only then call CommitDiff, eliminating
// the old cursor-before-delivery ordering.
func PendingDiff(account, board, kind, revision string, identities ...string) (DiffState, error) {
	keys := diffKeyPrefix(account, board, kind, revision)
	conn := connections.Redis()
	defer conn.Close()
	initialized, err := redis.Bool(conn.Do("EXISTS", keys+"initialized"))
	if err != nil {
		return DiffState{}, err
	}
	base, err := redis.Strings(conn.Do("SMEMBERS", keys+"base"))
	if err != nil {
		return DiffState{}, err
	}
	bench, err := redis.Strings(conn.Do("SMEMBERS", keys+"bench"))
	if err != nil {
		return DiffState{}, err
	}
	state := DiffState{Initialized: initialized}
	if !initialized {
		return state, nil
	}
	known := make(map[string]struct{}, len(base)+len(bench))
	for _, identity := range append(base, bench...) {
		known[identity] = struct{}{}
	}
	seen := make(map[string]struct{}, len(identities))
	for _, identity := range identities {
		identity = strings.TrimSpace(identity)
		if identity == "" {
			continue
		}
		if _, duplicate := seen[identity]; duplicate {
			continue
		}
		seen[identity] = struct{}{}
		if _, exists := known[identity]; !exists {
			state.New = append(state.New, identity)
		}
	}
	return state, nil
}

// CommitDiff advances a threshold revision after its outbox writes succeed.
func CommitDiff(account, board, kind, revision string, identities ...string) error {
	keys := diffKeyPrefix(account, board, kind, revision)
	conn := connections.Redis()
	defer conn.Close()
	if err := conn.Send("MULTI"); err != nil {
		return err
	}
	if len(identities) > 0 {
		args := redis.Args{}.Add(keys + "base")
		for _, identity := range identities {
			if identity = strings.TrimSpace(identity); identity != "" {
				args = args.Add(identity)
			}
		}
		if len(args) > 1 {
			if err := conn.Send("SADD", args...); err != nil {
				return err
			}
		}
	}
	if err := conn.Send("SET", keys+"initialized", "1"); err != nil {
		return err
	}
	values, err := redis.Values(conn.Do("EXEC"))
	if err != nil {
		return err
	}
	for _, value := range values {
		if transactionErr, ok := value.(redis.Error); ok {
			return transactionErr
		}
	}
	return nil
}

// DiffList is retained for legacy callers. New reliable paths should use
// PendingDiff + durable enqueue + CommitDiff with a threshold revision.
func DiffList(account, board, kind string, ids ...int) []int {
	identities := make([]string, 0, len(ids))
	for _, id := range ids {
		identities = append(identities, strconv.Itoa(id))
	}
	state, err := PendingDiff(account, board, kind, "legacy", identities...)
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
		return []int{}
	}
	if err := CommitDiff(account, board, kind, "legacy", identities...); err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
		return []int{}
	}
	if !state.Initialized {
		return []int{}
	}
	result := make([]int, 0, len(state.New))
	for _, identity := range state.New {
		id, err := strconv.Atoi(identity)
		if err == nil {
			result = append(result, id)
		}
	}
	return result
}

func DelDiffList(account, board, kind string) error {
	preKeyTemplate := prefix + account + ":" + board + ":" + kind + ":*"
	conn := connections.Redis()
	defer conn.Close()
	preKeys, err := redis.Strings(conn.Do("KEYS", preKeyTemplate))
	if len(preKeys) > 0 {
		_, err = conn.Do("DEL", redis.Args{}.AddFlat(preKeys)...)
	}
	if err != nil {
		log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
	}
	return err
}

func ReplaceBenchKeys() error {
	return ReplaceBenchKeysContext(context.Background())
}

func ReplaceBenchKeysContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	baseKeyTemplate := prefix + "*:*:*:*:base"
	conn := connections.Redis()
	defer conn.Close()
	baseKeys, err := redis.Strings(conn.Do("KEYS", baseKeyTemplate))
	if err != nil {
		return fmt.Errorf("list push-sum state for rotation: %w", err)
	}
	for _, baseKey := range baseKeys {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		key := strings.TrimSuffix(baseKey, "base") + "bench"
		if _, err = conn.Do("RENAME", baseKey, key); err != nil {
			log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
			return fmt.Errorf("rotate push-sum state %s: %w", baseKey, err)
		}
	}
	return nil
}

func RenameDiffListKeys(preBoard, postBoard string) error {
	keyTemplate := prefix + "*:" + preBoard + ":*"
	conn := connections.Redis()
	defer conn.Close()
	keys, err := redis.Strings(conn.Do("KEYS", keyTemplate))
	for _, key := range keys {
		if postBoard == "" {
			_, err = conn.Do("DEL", key)
			continue
		}
		newKey := strings.Replace(key, preBoard, postBoard, -1)
		bl, err := redis.Bool(conn.Do("EXISTS", newKey))
		if err == nil {
			if bl {
				_, err = conn.Do("DEL", key)
			} else {
				conn.Send("WATCH", key)
				conn.Send("MULTI")
				conn.Send("RENAME", key, newKey)
				_, err = conn.Do("EXEC")
			}
		}
		if err != nil {
			log.WithField("runtime", myutil.BasicRuntimeInfo()).WithError(err).Error()
		}
	}
	return err
}
