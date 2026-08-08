package pttaccess

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	redigo "github.com/garyburd/redigo/redis"
)

func TestCooldownLoadMissingAndAtomicMax(t *testing.T) {
	store, server := newTestStore(t, 100, 1000)
	ctx := context.Background()

	loaded, err := store.LoadCooldown(ctx)
	if err != nil {
		t.Fatalf("LoadCooldown() missing error = %v", err)
	}
	if !loaded.IsZero() {
		t.Fatalf("LoadCooldown() missing = %v, want zero", loaded)
	}

	base := time.Date(2026, 7, 13, 10, 0, 0, 0, time.UTC)
	deadlines := []time.Time{
		base.Add(5 * time.Minute),
		base.Add(30 * time.Minute),
		base.Add(10 * time.Minute),
		base.Add(2 * time.Hour),
		base.Add(time.Hour),
	}
	var wait sync.WaitGroup
	for index := 0; index < 40; index++ {
		deadline := deadlines[index%len(deadlines)]
		wait.Add(1)
		go func() {
			defer wait.Done()
			if extendErr := store.ExtendCooldown(ctx, deadline); extendErr != nil {
				t.Errorf("ExtendCooldown(%v) error = %v", deadline, extendErr)
			}
		}()
	}
	wait.Wait()

	loaded, err = store.LoadCooldown(ctx)
	if err != nil {
		t.Fatalf("LoadCooldown() error = %v", err)
	}
	want := base.Add(2 * time.Hour)
	if !loaded.Equal(want) {
		t.Fatalf("LoadCooldown() = %v, want %v", loaded, want)
	}
	if raw, getErr := server.Get(keyPrefix + ":cooldown"); getErr != nil || raw != fmt.Sprint(want.UnixMilli()) {
		t.Fatalf("stored cooldown = %q, %v", raw, getErr)
	}
}

func TestReserveRequestHourlyLimitAndReset(t *testing.T) {
	store, server := newTestStore(t, 2, 10)
	ctx := context.Background()
	now := time.Date(2026, 7, 13, 10, 15, 0, 0, time.UTC)

	for attempt := 1; attempt <= 2; attempt++ {
		retryAt, err := store.ReserveRequest(ctx, now)
		if err != nil || !retryAt.IsZero() {
			t.Fatalf("ReserveRequest() admitted attempt %d = (%v, %v)", attempt, retryAt, err)
		}
	}
	retryAt, err := store.ReserveRequest(ctx, now)
	if err != nil {
		t.Fatalf("ReserveRequest() denied error = %v", err)
	}
	wantReset := time.Date(2026, 7, 13, 11, 0, 0, 0, time.UTC)
	if !retryAt.Equal(wantReset) {
		t.Fatalf("ReserveRequest() retry = %v, want %v", retryAt, wantReset)
	}

	hourKey := store.hourKey(now.Unix() / hourWindowSeconds)
	dayKey := store.dayKey(now.Unix() / dayWindowSeconds)
	if value, getErr := server.Get(hourKey); getErr != nil || value != "2" {
		t.Fatalf("hour count after denial = %q, %v", value, getErr)
	}
	if value, getErr := server.Get(dayKey); getErr != nil || value != "2" {
		t.Fatalf("day count after denial = %q, %v", value, getErr)
	}

	if retryAt, err = store.ReserveRequest(ctx, wantReset); err != nil || !retryAt.IsZero() {
		t.Fatalf("ReserveRequest() after hour reset = (%v, %v)", retryAt, err)
	}
	if value, getErr := server.Get(dayKey); getErr != nil || value != "3" {
		t.Fatalf("day count after hour reset = %q, %v", value, getErr)
	}
}

func TestReserveRequestDailyLimitAndReset(t *testing.T) {
	store, server := newTestStore(t, 10, 2)
	ctx := context.Background()
	now := time.Date(2026, 7, 13, 23, 30, 0, 0, time.UTC)

	for attempt := 0; attempt < 2; attempt++ {
		if retryAt, err := store.ReserveRequest(ctx, now); err != nil || !retryAt.IsZero() {
			t.Fatalf("ReserveRequest() admitted = (%v, %v)", retryAt, err)
		}
	}
	retryAt, err := store.ReserveRequest(ctx, now)
	if err != nil {
		t.Fatalf("ReserveRequest() denied error = %v", err)
	}
	midnight := time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)
	if !retryAt.Equal(midnight) {
		t.Fatalf("ReserveRequest() retry = %v, want %v", retryAt, midnight)
	}

	hourKey := store.hourKey(now.Unix() / hourWindowSeconds)
	if value, getErr := server.Get(hourKey); getErr != nil || value != "2" {
		t.Fatalf("hour count changed on daily denial = %q, %v", value, getErr)
	}
	if retryAt, err = store.ReserveRequest(ctx, midnight); err != nil || !retryAt.IsZero() {
		t.Fatalf("ReserveRequest() after day reset = (%v, %v)", retryAt, err)
	}
}

func TestReserveRequestUsesLatestResetWhenBothWindowsAreFull(t *testing.T) {
	store, _ := newTestStore(t, 1, 1)
	ctx := context.Background()
	now := time.Date(2026, 7, 13, 10, 15, 0, 0, time.UTC)

	if retryAt, err := store.ReserveRequest(ctx, now); err != nil || !retryAt.IsZero() {
		t.Fatalf("ReserveRequest() admitted = (%v, %v)", retryAt, err)
	}
	retryAt, err := store.ReserveRequest(ctx, now)
	if err != nil {
		t.Fatalf("ReserveRequest() denied error = %v", err)
	}
	want := time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)
	if !retryAt.Equal(want) {
		t.Fatalf("ReserveRequest() retry = %v, want later daily reset %v", retryAt, want)
	}
}

func TestReserveRequestConcurrentAdmissionIsAtomic(t *testing.T) {
	store, _ := newTestStore(t, 17, 100)
	ctx := context.Background()
	now := time.Date(2026, 7, 13, 10, 15, 0, 0, time.UTC)
	var admitted atomic.Int64
	var denied atomic.Int64
	var wait sync.WaitGroup
	for index := 0; index < 100; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			retryAt, err := store.ReserveRequest(ctx, now)
			if err != nil {
				t.Errorf("ReserveRequest() error = %v", err)
				return
			}
			if retryAt.IsZero() {
				admitted.Add(1)
			} else {
				denied.Add(1)
			}
		}()
	}
	wait.Wait()
	if admitted.Load() != 17 || denied.Load() != 83 {
		t.Fatalf("admitted/denied = %d/%d, want 17/83", admitted.Load(), denied.Load())
	}
}

func TestRecordBlockUsesExactSlidingWindowAndAtomicallyExtendsCooldown(t *testing.T) {
	store, server := newTestStore(t, 10, 100)
	ctx := context.Background()
	now := time.Date(2026, 7, 13, 10, 0, 0, 0, time.UTC)
	minimum := 15 * time.Minute
	breaker := 24 * time.Hour

	count, until, err := store.RecordBlock(ctx, now, minimum, 3, breaker)
	if err != nil || count != 1 || !until.Equal(now.Add(minimum)) {
		t.Fatalf("RecordBlock() first = (%d, %v, %v)", count, until, err)
	}
	count, until, err = store.RecordBlock(ctx, now.Add(23*time.Hour), minimum, 3, breaker)
	if err != nil || count != 2 || !until.Equal(now.Add(23*time.Hour+minimum)) {
		t.Fatalf("RecordBlock() second = (%d, %v, %v)", count, until, err)
	}

	// The first event is now outside the actual 24-hour interval. A rolling TTL
	// counter would incorrectly report three and open the breaker here.
	count, until, err = store.RecordBlock(ctx, now.Add(46*time.Hour), minimum, 3, breaker)
	if err != nil || count != 2 || !until.Equal(now.Add(46*time.Hour+minimum)) {
		t.Fatalf("RecordBlock() exact-window third = (%d, %v, %v), want count 2", count, until, err)
	}

	count, until, err = store.RecordBlock(ctx, now.Add(46*time.Hour+time.Minute), minimum, 3, breaker)
	wantBreaker := now.Add(70*time.Hour + time.Minute)
	if err != nil || count != 3 || !until.Equal(wantBreaker) {
		t.Fatalf("RecordBlock() breaker = (%d, %v, %v), want (3, %v, nil)", count, until, err, wantBreaker)
	}
	stored, loadErr := store.LoadCooldown(ctx)
	if loadErr != nil || !stored.Equal(wantBreaker) {
		t.Fatalf("atomic cooldown = (%v, %v), want %v", stored, loadErr, wantBreaker)
	}
	connection, dialErr := redigo.Dial("tcp", server.Addr())
	if dialErr != nil {
		t.Fatal(dialErr)
	}
	defer connection.Close()
	cardinality, cardErr := redigo.Int(connection.Do("ZCARD", store.blockKey()))
	if cardErr != nil || cardinality != 3 {
		t.Fatalf("block event cardinality = %d, %v", cardinality, cardErr)
	}
}

func TestStrictConfigurationContextAndRedisErrors(t *testing.T) {
	for _, config := range []RedisConfig{
		{},
		{HourlyLimit: -1, DailyLimit: 1},
		{HourlyLimit: 1, DailyLimit: -1},
	} {
		if _, err := NewRedis(config); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("NewRedis(%+v) error = %v, want ErrInvalidConfig", config, err)
		}
	}

	dialError := errors.New("dial failed")
	store, err := NewRedis(RedisConfig{
		HourlyLimit: 1,
		DailyLimit:  1,
		Connect: func() (redigo.Conn, error) {
			return nil, dialError
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = store.LoadCooldown(context.Background()); !errors.Is(err, dialError) {
		t.Fatalf("LoadCooldown() dial error = %v", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = store.ReserveRequest(canceled, time.Now()); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReserveRequest() canceled error = %v", err)
	}
	if err = store.ExtendCooldown(context.Background(), time.Time{}); !errors.Is(err, ErrInvalidTime) {
		t.Fatalf("ExtendCooldown() zero error = %v", err)
	}
}

func TestCorruptAndWrongTypeDataReturnErrors(t *testing.T) {
	store, server := newTestStore(t, 10, 100)
	ctx := context.Background()
	now := time.Date(2026, 7, 13, 10, 0, 0, 0, time.UTC)

	server.Set(store.cooldownKey(), "not-an-integer")
	if _, err := store.LoadCooldown(ctx); !errors.Is(err, ErrCorruptData) {
		t.Fatalf("LoadCooldown() corrupt error = %v", err)
	}
	server.Set(store.cooldownKey(), "1e3")
	if err := store.ExtendCooldown(ctx, now.Add(time.Hour)); !errors.Is(err, ErrCorruptData) {
		t.Fatalf("ExtendCooldown() noncanonical error = %v", err)
	}
	server.Del(store.cooldownKey())
	server.Set(store.hourKey(now.Unix()/hourWindowSeconds), "1e0")
	if _, err := store.ReserveRequest(ctx, now); !errors.Is(err, ErrCorruptData) {
		t.Fatalf("ReserveRequest() corrupt error = %v", err)
	}
	server.Del(store.hourKey(now.Unix() / hourWindowSeconds))
	server.Lpush(store.blockKey(), "wrong-type")
	if _, _, err := store.RecordBlock(ctx, now, 15*time.Minute, 3, 24*time.Hour); !errors.Is(err, ErrCorruptData) {
		t.Fatal("RecordBlock() wrong-type error = nil")
	}
}

func newTestStore(t *testing.T, hourlyLimit, dailyLimit int64) (*Redis, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	store, err := NewRedis(RedisConfig{
		HourlyLimit: hourlyLimit,
		DailyLimit:  dailyLimit,
		Connect: func() (redigo.Conn, error) {
			return redigo.Dial("tcp", server.Addr())
		},
	})
	if err != nil {
		t.Fatalf("NewRedis() error = %v", err)
	}
	return store, server
}
