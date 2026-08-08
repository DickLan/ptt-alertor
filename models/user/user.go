package user

import (
	"context"
	"errors"
	"regexp"
	"time"

	"github.com/Ptt-Alertor/ptt-alertor/models/subscription"
)

type User struct {
	Enable     bool      `json:"enable"`
	CreateTime time.Time `json:"createTime"`
	UpdateTime time.Time `json:"updateTime"`
	Profile    `json:"Profile"`
	Subscribes subscription.Subscriptions
	drive      Driver
}

type Profile struct {
	Account         string `json:"account"`
	Type            string `json:"type,omitempty"`
	Discord         bool   `json:"discord,omitempty"`
	Email           string `json:"email"`
	Line            string `json:"line"`
	LineAccessToken string `json:"lineAccessToken"`
	Messenger       string `json:"messenger"`
	Telegram        string `json:"telegram"`
	TelegramChat    int64  `json:"telegramChat"`
}

type Driver interface {
	List() (accounts []string)
	Exist(account string) bool
	Save(account string, user interface{}) error
	Update(account string, user interface{}) error
	Find(account string, user *User)
}

var ErrAccountEmpty = errors.New("account can not be empty")
var ErrAccountInvalid = errors.New("account must be 1-128 URL-safe ASCII characters")
var ErrUserAlreadyExists = errors.New("user already exist")
var ErrUserNotExist = errors.New("user not exist")
var ErrConcurrentUpdate = errors.New("user changed concurrently")
var ErrSubscriptionTransactionUnsupported = errors.New("user driver does not support atomic subscription updates")
var ErrInvalidSubscriptionIndex = errors.New("subscription index has an invalid Redis type")

var accountPattern = regexp.MustCompile(`^[A-Za-z0-9._@+~-]{1,128}$`)

func validateAccount(account string) error {
	if account == "" {
		return ErrAccountEmpty
	}
	// Accounts are used both as Redis identifiers and as one httprouter path
	// segment. Reject separators, escapes, whitespace, controls, and ambiguous
	// query/fragment characters so every stored user remains addressable.
	if !accountPattern.MatchString(account) {
		return ErrAccountInvalid
	}
	return nil
}

func NewUser(drive Driver) *User {
	return &User{
		drive: drive,
	}
}

func (u User) All() (us []*User) {
	return u.AllContext(context.Background())
}

// AllContext stops walking user records when a scheduled job is canceled.
func (u User) AllContext(ctx context.Context) (us []*User) {
	accounts := u.drive.List()
	for _, account := range accounts {
		if ctx.Err() != nil {
			return us
		}
		user := u.Find(account)
		us = append(us, &user)
	}
	return us
}

func (u User) Save() error {
	if err := validateAccount(u.Profile.Account); err != nil {
		return err
	}

	if !u.Profile.Discord {
		return errors.New("discord notifications must be enabled")
	}
	if u.drive.Exist(u.Profile.Account) {
		return ErrUserAlreadyExists
	}
	u.CreateTime = time.Now()
	u.UpdateTime = time.Now()

	return u.drive.Save(u.Profile.Account, u)
}

func (u User) Update() error {
	if err := validateAccount(u.Profile.Account); err != nil {
		return err
	}
	if !u.drive.Exist(u.Profile.Account) {
		return ErrUserNotExist
	}

	u.UpdateTime = time.Now()
	return u.drive.Update(u.Profile.Account, u)
}

type compareAndSwapDriver interface {
	CompareAndSwap(account string, previous, next User) error
}

type subscriptionUpdateDriver interface {
	UpdateSubscriptions(account string, previous, next User) error
}

type errorFindingDriver interface {
	FindE(account string, user *User) error
}

// Clone returns an independent copy suitable for calculating subscription
// index changes after mutating the original User.
func (u User) Clone() User {
	clone := u
	if u.Subscribes == nil {
		return clone
	}
	clone.Subscribes = make(subscription.Subscriptions, len(u.Subscribes))
	for index, sub := range u.Subscribes {
		copySub := sub
		copySub.Keywords = append(copySub.Keywords[:0:0], sub.Keywords...)
		copySub.Authors = append(copySub.Authors[:0:0], sub.Authors...)
		copySub.Articles = append(copySub.Articles[:0:0], sub.Articles...)
		clone.Subscribes[index] = copySub
	}
	return clone
}

// UpdateIfUnchanged updates a user document only if it still matches the
// caller's snapshot. It prevents a profile PUT from overwriting subscriptions
// committed by a concurrent command request.
func (u *User) UpdateIfUnchanged(previous User) error {
	if err := validateAccount(u.Profile.Account); err != nil {
		return err
	}
	updater, ok := u.drive.(compareAndSwapDriver)
	if !ok {
		return ErrSubscriptionTransactionUnsupported
	}
	u.UpdateTime = time.Now()
	return updater.CompareAndSwap(u.Profile.Account, previous, *u)
}

// UpdateSubscriptions atomically commits the user JSON document and every
// derived Redis notification index.
func (u *User) UpdateSubscriptions(previous User) error {
	if err := validateAccount(u.Profile.Account); err != nil {
		return err
	}
	updater, ok := u.drive.(subscriptionUpdateDriver)
	if !ok {
		return ErrSubscriptionTransactionUnsupported
	}
	u.UpdateTime = time.Now()
	return updater.UpdateSubscriptions(u.Profile.Account, previous, *u)
}

func (u User) Find(account string) User {
	u.drive.Find(account, &u)
	return u
}

// FindE is the error-aware lookup used by reliable background jobs. Legacy
// drivers without an error-returning API retain their compatibility behavior.
func (u User) FindE(account string) (User, error) {
	if finder, ok := u.drive.(errorFindingDriver); ok {
		if err := finder.FindE(account, &u); err != nil {
			return u, err
		}
		return u, nil
	}
	u.drive.Find(account, &u)
	return u, nil
}
