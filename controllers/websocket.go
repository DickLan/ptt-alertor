package controllers

import (
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	log "github.com/Ptt-Alertor/logrus"
	"github.com/Ptt-Alertor/ptt-alertor/connections"
	"github.com/garyburd/redigo/redis"
	"github.com/julienschmidt/httprouter"
	"golang.org/x/net/websocket"
)

const maxCounterWebSockets = 64

// counterHub uses one Redis subscription for the whole process and fans the
// latest counter value out to a bounded set of browser connections.
type counterHub struct {
	mu         sync.Mutex
	clients    map[chan []byte]struct{}
	maxClients int
	startOnce  sync.Once
}

var counterUpdates = newCounterHub(maxCounterWebSockets)

func newCounterHub(maxClients int) *counterHub {
	return &counterHub{
		clients:    make(map[chan []byte]struct{}),
		maxClients: maxClients,
	}
}

func (h *counterHub) start() {
	h.startOnce.Do(func() { go h.subscribe() })
}

func (h *counterHub) add() (chan []byte, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.clients) >= h.maxClients {
		return nil, false
	}
	updates := make(chan []byte, 1)
	h.clients[updates] = struct{}{}
	return updates, true
}

func (h *counterHub) remove(updates chan []byte) {
	h.mu.Lock()
	delete(h.clients, updates)
	h.mu.Unlock()
}

func (h *counterHub) broadcast(data []byte) {
	payload := append([]byte(nil), data...)
	h.mu.Lock()
	defer h.mu.Unlock()
	for updates := range h.clients {
		select {
		case updates <- payload:
		default:
			// Only the newest total matters. Replace a queued stale value rather
			// than allowing one slow browser to block all other clients.
			select {
			case <-updates:
			default:
			}
			select {
			case updates <- payload:
			default:
			}
		}
	}
}

func (h *counterHub) subscribe() {
	delay := time.Second
	for {
		conn, err := connections.RedisPubSub()
		if err == nil {
			psc := redis.PubSubConn{Conn: conn}
			err = psc.Subscribe("alert-counter")
			if err == nil {
				delay = time.Second
				for err == nil {
					switch value := psc.Receive().(type) {
					case redis.Message:
						h.broadcast(value.Data)
					case redis.Subscription:
						// Subscription acknowledgement; no client update needed.
					case error:
						err = value
					}
				}
			}
			_ = conn.Close()
		}
		log.WithError(err).Warn("Redis counter subscription disconnected; retrying")
		time.Sleep(delay)
		if delay < 30*time.Second {
			delay *= 2
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
		}
	}
}

func sameWebSocketOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	parsed, err := url.Parse(origin)
	return err == nil && parsed.Host != "" && strings.EqualFold(parsed.Host, r.Host)
}

// WebSocket upgrades same-origin browser requests and streams counter updates.
// Connections are capped, and one read pump detects clients that disconnect
// while no counter messages are being published.
func WebSocket(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	if !sameWebSocketOrigin(r) {
		http.Error(w, "websocket origin is not allowed", http.StatusForbidden)
		return
	}
	updates, ok := counterUpdates.add()
	if !ok {
		http.Error(w, "too many websocket connections", http.StatusServiceUnavailable)
		return
	}
	defer counterUpdates.remove(updates)
	counterUpdates.start()
	websocket.Handler(func(ws *websocket.Conn) {
		counterHandler(ws, updates)
	}).ServeHTTP(w, r)
}

func counterHandler(ws *websocket.Conn, updates <-chan []byte) {
	disconnected := make(chan struct{})
	go func() {
		defer close(disconnected)
		for {
			var ignored []byte
			if err := websocket.Message.Receive(ws, &ignored); err != nil {
				return
			}
		}
	}()
	defer ws.Close()
	for {
		select {
		case data := <-updates:
			if err := websocket.Message.Send(ws, string(data)); err != nil {
				return
			}
		case <-disconnected:
			return
		}
	}
}
