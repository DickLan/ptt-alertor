package user

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestUser_All(t *testing.T) {
	tests := []struct {
		name   string
		u      User
		wantUs []*User
	}{
		{"ok", User{drive: new(Mock)}, []*User{
			&User{Profile: Profile{Account: "dinos80152@gmail.com"}, drive: new(Mock)},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotUs := tt.u.All(); !reflect.DeepEqual(gotUs, tt.wantUs) {
				t.Errorf("User.All() = %v, want %v", gotUs[0], tt.wantUs[0])
			}
		})
	}
}

func TestUser_Save(t *testing.T) {
	tests := []struct {
		name    string
		u       User
		wantErr bool
	}{
		{"discord", User{Profile: Profile{Account: "discord-user", Discord: true}, drive: new(Mock)}, false},
		{"legacy only", User{Profile: Profile{Account: "email-user", Email: "user@example.com"}, drive: new(Mock)}, true},
		{"duplicate", User{Profile: Profile{Account: "dinos80152@gmail.com", Discord: true}, drive: new(Mock)}, true},
		{"notifications disabled", User{Profile: Profile{Account: "disabled-user"}, drive: new(Mock)}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.u.Save(); (err != nil) != tt.wantErr {
				t.Errorf("User.Save() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestUserSaveRejectsUnaddressableAccount(t *testing.T) {
	invalid := []string{
		"unreachable/account",
		"back\\slash",
		"query?value",
		"fragment#value",
		"encoded%2Fslash",
		" leading-space",
		"trailing-space ",
		"line\nbreak",
		strings.Repeat("a", 129),
	}
	for _, account := range invalid {
		u := User{Profile: Profile{Account: account, Discord: true}, drive: new(Mock)}
		if err := u.Save(); !errors.Is(err, ErrAccountInvalid) {
			t.Errorf("Save() account %q error = %v, want ErrAccountInvalid", account, err)
		}
	}
}

func TestUser_Update(t *testing.T) {
	tests := []struct {
		name    string
		u       User
		wantErr bool
	}{
		{"ok", User{Profile: Profile{Account: "dinos80152@gmail.com"}, drive: new(Mock)}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.u.Update(); (err != nil) != tt.wantErr {
				t.Errorf("User.Update() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestUser_Find(t *testing.T) {
	type args struct {
		account string
	}
	tests := []struct {
		name string
		u    User
		args args
		want User
	}{
		{"ok", User{drive: new(Mock)}, args{"dinos80152@gmail.com"},
			User{Profile: Profile{Account: "dinos80152@gmail.com"}, drive: new(Mock)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.u.Find(tt.args.account); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("User.Find() = %v, want %v", got, tt.want)
			}
		})
	}
}
