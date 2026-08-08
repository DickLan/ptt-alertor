package subscription

import "testing"

func TestVerifiedMutationsDoNotAppendEmptySubscription(t *testing.T) {
	for _, test := range []struct {
		name  string
		apply func(*Subscriptions)
	}{
		{
			name: "add",
			apply: func(subscriptions *Subscriptions) {
				subscriptions.AddVerified(Subscription{Board: "stock"})
			},
		},
		{
			name: "update",
			apply: func(subscriptions *Subscriptions) {
				subscriptions.UpdateVerified(Subscription{Board: "stock"})
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var subscriptions Subscriptions
			test.apply(&subscriptions)
			if len(subscriptions) != 0 {
				t.Fatalf("subscriptions = %#v, want empty", subscriptions)
			}
		})
	}
}

func TestUpdateVerifiedRemovesSubscriptionWhenThresholdsBecomeZero(t *testing.T) {
	subscriptions := Subscriptions{{Board: "stock", PushSum: PushSum{Up: 10}}}
	subscriptions.UpdateVerified(Subscription{Board: "stock", PushSum: PushSum{}})
	if len(subscriptions) != 0 {
		t.Fatalf("subscriptions = %#v, want empty", subscriptions)
	}
}
