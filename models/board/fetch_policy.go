package board

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Ptt-Alertor/ptt-alertor/connections"
	redigo "github.com/garyburd/redigo/redis"
)

const (
	boardFetchPolicyPrefix = "ptt:board-fetch:"
	htmlOnlyTTL            = 6 * time.Hour
	boardFailureBackoff    = 15 * time.Minute
)

var ErrBoardFetchBackoff = errors.New("PTT board fetch is in persistent backoff")

type boardFetchPolicy struct {
	HTMLOnly bool
	Backoff  bool
}

var (
	loadBoardFetchPolicy = redisBoardFetchPolicy
	markBoardHTMLOnly    = redisMarkBoardHTMLOnly
	markBoardBackoff     = redisMarkBoardBackoff
)

func boardPolicyKey(boardName, suffix string) string {
	return boardFetchPolicyPrefix + strings.ToLower(strings.TrimSpace(boardName)) + ":" + suffix
}

func redisBoardFetchPolicy(boardName string) (boardFetchPolicy, error) {
	connection := connections.Redis()
	defer connection.Close()
	mode, err := redigo.String(connection.Do("GET", boardPolicyKey(boardName, "mode")))
	if errors.Is(err, redigo.ErrNil) {
		mode = ""
	} else if err != nil {
		return boardFetchPolicy{}, fmt.Errorf("load board fetch mode: %w", err)
	}
	if mode != "" && mode != "html" {
		return boardFetchPolicy{}, errors.New("load board fetch mode: corrupt value")
	}
	backoff, err := redigo.Bool(connection.Do("EXISTS", boardPolicyKey(boardName, "backoff")))
	if err != nil {
		return boardFetchPolicy{}, fmt.Errorf("load board fetch backoff: %w", err)
	}
	return boardFetchPolicy{HTMLOnly: mode == "html", Backoff: backoff}, nil
}

func redisMarkBoardHTMLOnly(boardName string) error {
	connection := connections.Redis()
	defer connection.Close()
	_, err := connection.Do("SET", boardPolicyKey(boardName, "mode"), "html", "PX", htmlOnlyTTL.Milliseconds())
	if err != nil {
		return fmt.Errorf("persist board HTML-only mode: %w", err)
	}
	return nil
}

func redisMarkBoardBackoff(boardName string) error {
	connection := connections.Redis()
	defer connection.Close()
	_, err := connection.Do("SET", boardPolicyKey(boardName, "backoff"), "1", "PX", boardFailureBackoff.Milliseconds())
	if err != nil {
		return fmt.Errorf("persist board fetch backoff: %w", err)
	}
	return nil
}
