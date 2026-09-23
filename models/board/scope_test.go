package board

import "testing"

func TestPollingAllowed(t *testing.T) {
	t.Setenv(pollingAllowlistEnvironment, " HardwareSale,MacShop,PC_Shopping ")
	for _, name := range []string{"hardwaresale", "MACSHOP", "pc_shopping"} {
		if !PollingAllowed(name) {
			t.Errorf("PollingAllowed(%q) = false", name)
		}
	}
	for _, name := range []string{"stock", "", "  "} {
		if PollingAllowed(name) {
			t.Errorf("PollingAllowed(%q) = true", name)
		}
	}
}

func TestPollingAllowedIsUnrestrictedWhenUnsetOrEmpty(t *testing.T) {
	for _, value := range []string{"", "   "} {
		t.Run(value, func(t *testing.T) {
			t.Setenv(pollingAllowlistEnvironment, value)
			for _, board := range []string{"stock", "HardwareSale", "anything"} {
				if !PollingAllowed(board) {
					t.Fatalf("PollingAllowed(%q) = false with empty allowlist", board)
				}
			}
		})
	}
}
