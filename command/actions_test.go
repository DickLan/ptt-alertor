package command

import (
	"context"
	"errors"
	"testing"

	"github.com/Ptt-Alertor/ptt-alertor/models/subscription"
	"github.com/Ptt-Alertor/ptt-alertor/models/user"
)

func TestAddKeywordsRejectsMalformedAuthorTitleRules(t *testing.T) {
	for _, rule := range []string{
		"author:&賣",
		"author:lushin&",
		"author:lushin&&賣",
	} {
		t.Run(rule, func(t *testing.T) {
			current := user.User{}
			err := addKeywords(
				context.Background(),
				&current,
				subscription.Subscription{Board: "hardwaresale"},
				rule,
			)
			if !errors.Is(err, errInvalidAuthorTitleKeyword) {
				t.Fatalf("addKeywords(%q) error = %v, want malformed-rule error", rule, err)
			}
			if len(current.Subscribes) != 0 {
				t.Fatalf("addKeywords(%q) changed subscriptions: %#v", rule, current.Subscribes)
			}
		})
	}
}

func TestAuthorPrefixOutsideValidCompoundRemainsAPlainKeyword(t *testing.T) {
	for _, rule := range []string{"author:lushin", "賣&author:lushin"} {
		if err := validateAuthorTitleKeywords([]string{rule}); err != nil {
			t.Errorf("validateAuthorTitleKeywords(%q) error = %v, want plain keyword allowed", rule, err)
		}
	}
}

func TestRemoveKeywordsAllowsMalformedLegacyAuthorTitleRule(t *testing.T) {
	const malformed = "author:lushin&"
	current := user.User{Subscribes: subscription.Subscriptions{{
		Board:    "hardwaresale",
		Keywords: []string{malformed, "ddr4"},
	}}}

	err := removeKeywords(
		context.Background(),
		&current,
		subscription.Subscription{Board: "hardwaresale"},
		malformed,
	)
	if err != nil {
		t.Fatalf("removeKeywords() error = %v", err)
	}
	if len(current.Subscribes) != 1 || len(current.Subscribes[0].Keywords) != 1 ||
		current.Subscribes[0].Keywords[0] != "ddr4" {
		t.Fatalf("subscriptions after removing malformed rule = %#v, want only ddr4", current.Subscribes)
	}
}
