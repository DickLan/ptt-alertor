package article

import "testing"

var test = `{"ID":1602349944,"code":"M.1602349944.A.250","Title":"[心得] 我不是耳機，是個工具!Nathaniel Baldwin","Link":"https://www.ptt.cc/bbs/Headphone/M.1602349944.A.250.html","pushList":[{"Tag":"推 ","UserID":"Yazilightar","Content":": 向老前輩致敬XDD","DateTime":"2020-10-11T15:46:00+08:00"}],"lastPushDateTime":"2020-10-11T15:46:00+08:00","board":"Headphone"}`

func TestParseCodeAndID(t *testing.T) {
	tests := []struct {
		name     string
		link     string
		wantCode string
		wantID   int
	}{
		{
			name:     "modern M article URL",
			link:     "https://www.ptt.cc/bbs/NBA/M.1700000000.A.1B2.html",
			wantCode: "M.1700000000.A.1B2",
			wantID:   1700000000,
		},
		{
			name:     "legacy G article URL with query",
			link:     "https://www.ptt.cc/bbs/Old/G.1234567890.A.ABC.html?from=search#push",
			wantCode: "G.1234567890.A.ABC",
			wantID:   1234567890,
		},
		{
			name:     "bare article code",
			link:     "M.42.A.00F",
			wantCode: "M.42.A.00F",
			wantID:   42,
		},
		{
			name:   "old ID parser compatibility",
			link:   "https://www.ptt.cc/bbs/Test/M.99.B.legacy.html",
			wantID: 99,
		},
		{name: "not an article", link: "https://www.ptt.cc/bbs/NBA/index.html"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := Article{}
			if got := value.ParseCode(test.link); got != test.wantCode {
				t.Errorf("ParseCode(%q) = %q, want %q", test.link, got, test.wantCode)
			}
			if got := value.ParseID(test.link); got != test.wantID {
				t.Errorf("ParseID(%q) = %d, want %d", test.link, got, test.wantID)
			}
		})
	}
}

func TestIdentityUsesCodeOrLinkButNeverTimestampAlone(t *testing.T) {
	tests := []struct {
		name    string
		article Article
		want    string
	}{
		{
			name: "explicit code has priority",
			article: Article{
				ID:   1,
				Code: "M.2.A.002",
				Link: "https://www.ptt.cc/bbs/NBA/M.1.A.001.html",
			},
			want: "M.2.A.002",
		},
		{
			name:    "legacy cached link is canonicalized to code",
			article: Article{ID: 1, Link: "https://www.ptt.cc/bbs/NBA/M.1.A.001.html"},
			want:    "M.1.A.001",
		},
		{
			name:    "non PTT link remains a fallback identity",
			article: Article{ID: 1, Link: " https://example.com/article/one "},
			want:    "https://example.com/article/one",
		},
		{
			name:    "timestamp ID is not an identity",
			article: Article{ID: 1700000000},
			want:    "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.article.Identity(); got != test.want {
				t.Errorf("Identity() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestTitleKeywordAndAuthorUseCaseInsensitiveSubstringMatching(t *testing.T) {
	value := Article{
		Title:  "[新聞] TSMC announces a new process",
		Author: "SomeAuthor",
	}

	if !value.MatchKeyword("tsmc") {
		t.Fatal("MatchKeyword() should match a case-insensitive title substring")
	}
	if value.MatchKeyword("TSMC earnings") {
		t.Fatal("MatchKeyword() matched text that is not contained in the title")
	}
	if !value.MatchAuthor("AUTH") {
		t.Fatal("MatchAuthor() should match a case-insensitive author substring")
	}
	if value.MatchAuthor("other") {
		t.Fatal("MatchAuthor() matched text that is not contained in the author")
	}
	if value.MatchAuthor("   ") {
		t.Fatal("MatchAuthor() must not treat an empty author rule as matching every article")
	}
}

func TestCompoundKeywordCanRequireAuthorAndTitle(t *testing.T) {
	value := Article{
		Title:  "[賣] DDR4 3200 記憶體",
		Author: "LuShIn",
	}

	tests := []struct {
		name string
		rule string
		want bool
	}{
		{name: "author then multiple title terms", rule: "author:LUSH&賣&ddr4", want: true},
		{name: "title then author remains title only", rule: "DDR4&author:LUSH", want: false},
		{name: "wrong author", rule: "author:someone-else&賣", want: false},
		{name: "wrong title", rule: "author:lushin&徵", want: false},
		{name: "empty author cannot match", rule: "author:&賣", want: false},
		{name: "empty final title cannot match", rule: "author:lushin&", want: false},
		{name: "empty middle title cannot match", rule: "author:lushin&&賣", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := value.MatchKeyword(test.rule); got != test.want {
				t.Errorf("MatchKeyword(%q) = %t, want %t", test.rule, got, test.want)
			}
		})
	}

	// A standalone keyword keeps its historical title-only meaning. The
	// author: prefix is special only at the beginning of an AND expression.
	plain := Article{Title: "[公告] author:lushin", Author: "someone-else"}
	if !plain.MatchKeyword("author:lushin") {
		t.Fatal("standalone plain keyword no longer matches the title")
	}
}
