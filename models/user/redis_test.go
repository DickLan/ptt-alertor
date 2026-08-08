package user

import (
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/garyburd/redigo/redis"
)

var s *miniredis.Miniredis
var err error

func TestMain(m *testing.M) {
	// setup
	s, err = miniredis.Run()
	if err != nil {
		panic(err)
	}

	connectRedis = func() redis.Conn {
		conn, err := redis.Dial("tcp", s.Addr())
		if err != nil {
			panic(err)
		}
		return conn
	}

	// run test
	v := m.Run()

	// teardown
	s.Close()
	os.Exit(v)
}

func TestRedis_List(t *testing.T) {
	s.FlushAll()
	s.Set("user:dinos80152", `{"account":"dinos80152"}`)

	tests := []struct {
		name         string
		r            Redis
		wantAccounts []string
	}{
		{"dinos80152", Redis{}, []string{"dinos80152"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotAccounts := tt.r.List(); !reflect.DeepEqual(gotAccounts, tt.wantAccounts) {
				t.Errorf("Redis.List() = %v, want %v", gotAccounts, tt.wantAccounts)
			}
		})
	}
}

func TestRedis_Exist(t *testing.T) {
	s.FlushAll()
	s.Set("user:dinos80152", `{"account":"dinos80152"}`)

	type args struct {
		account string
	}
	tests := []struct {
		name string
		r    Redis
		args args
		want bool
	}{
		{"dinos80152", Redis{}, args{"dinos80152"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.r.Exist(tt.args.account); got != tt.want {
				t.Errorf("Redis.Exist() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRedis_Save(t *testing.T) {
	s.FlushAll()
	type args struct {
		account string
		data    interface{}
	}
	tests := []struct {
		name    string
		r       Redis
		args    args
		wantErr bool
	}{
		{"ok", Redis{}, args{"new-user", User{Profile: Profile{Account: "new-user"}}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.Save(tt.args.account, tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("Redis.Save() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRedisSaveAndUpdateReportUnappliedConditions(t *testing.T) {
	s.FlushAll()
	r := Redis{}
	u := User{Profile: Profile{Account: "duplicate"}}
	if err := r.Save("duplicate", u); err != nil {
		t.Fatalf("first Save() error = %v", err)
	}
	if err := r.Save("duplicate", u); !errors.Is(err, ErrUserAlreadyExists) {
		t.Fatalf("duplicate Save() error = %v, want ErrUserAlreadyExists", err)
	}
	if err := r.Update("missing", u); !errors.Is(err, ErrUserNotExist) {
		t.Fatalf("missing Update() error = %v, want ErrUserNotExist", err)
	}
}

func TestRedis_Update(t *testing.T) {
	s.FlushAll()
	s.Set("user:dinos80152", `{"account":"dinos80152"}`)
	type args struct {
		account string
		user    interface{}
	}
	tests := []struct {
		name    string
		r       Redis
		args    args
		wantErr bool
	}{
		{"ok", Redis{}, args{"dinos80152", User{
			Enable: true,
			Profile: Profile{
				Account: "dinos80152",
			}}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.Update(tt.args.account, tt.args.user); (err != nil) != tt.wantErr {
				t.Errorf("Redis.Update() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRedis_Find(t *testing.T) {
	s.FlushAll()
	s.Set("user:dinos80152", `{"Profile":{"account":"dinos80152"}}`)
	type args struct {
		account string
		user    *User
	}
	tests := []struct {
		name string
		r    Redis
		args args
	}{
		{"ok", Redis{}, args{"dinos80152", &User{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.r.Find(tt.args.account, tt.args.user)
		})
	}
}

func TestRedisFindEReturnsJSONCorruption(t *testing.T) {
	s.FlushAll()
	s.Set("user:broken", `{not-json`)
	var got User
	if err := (Redis{}).FindE("broken", &got); err == nil {
		t.Fatal("FindE() error = nil, want JSON error")
	}
}

func TestRedisFindEMissingIsNotAnError(t *testing.T) {
	s.FlushAll()
	var got User
	if err := (Redis{}).FindE("missing", &got); err != nil {
		t.Fatalf("FindE() error = %v", err)
	}
	if got.Profile.Account != "" {
		t.Fatalf("missing user = %#v", got)
	}
}
