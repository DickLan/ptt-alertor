package command

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/Ptt-Alertor/ptt-alertor/models"
	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	"github.com/Ptt-Alertor/ptt-alertor/models/user"
	"github.com/Ptt-Alertor/ptt-alertor/ptt/web"
	"github.com/alicebob/miniredis"
	"github.com/garyburd/redigo/redis"
)

var commandRedis *miniredis.Miniredis

func TestMain(m *testing.M) {
	server, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	commandRedis = server
	host, port, err := net.SplitHostPort(server.Addr())
	if err != nil {
		panic(err)
	}
	_ = os.Setenv("REDIS_ENDPOINT", host)
	_ = os.Setenv("REDIS_PORT", port)
	_ = os.Setenv("STORAGE_BACKEND", "redis")
	code := m.Run()
	server.Close()
	os.Exit(code)
}

func saveCommandUser(t *testing.T, account string) {
	t.Helper()
	u := models.User()
	u.Enable = true
	u.Profile = user.Profile{Account: account, Discord: true}
	if err := u.Save(); err != nil {
		t.Fatalf("save user: %v", err)
	}
}

func seedCommandBoards(t *testing.T, boards ...string) {
	t.Helper()
	conn, err := redis.Dial("tcp", commandRedis.Addr())
	if err != nil {
		t.Fatalf("dial command Redis: %v", err)
	}
	defer conn.Close()
	args := redis.Args{}.Add("boards").AddFlat(boards)
	if _, err := conn.Do("SADD", args...); err != nil {
		t.Fatalf("seed boards: %v", err)
	}
}

func TestCommentCommandFetchesArticleOnceAndPersistsOnlySuccess(t *testing.T) {
	commandRedis.FlushAll()
	u := models.User()
	u.Enable = true
	u.Profile = user.Profile{Account: "discord-main", Discord: true}
	if err := u.Save(); err != nil {
		t.Fatalf("save user: %v", err)
	}

	originalFetch := fetchArticleFromPTT
	defer func() { fetchArticleFromPTT = originalFetch }()
	calls := 0
	fetchArticleFromPTT = func(ctx context.Context, board, code string) (article.Article, error) {
		calls++
		return article.Article{
			ID: 1, Board: board, Code: code, Title: "article", Link: "https://www.ptt.cc/bbs/NBA/" + code + ".html",
		}, nil
	}

	result := HandleCommandContext(context.Background(), "新增推文 https://www.ptt.cc/bbs/NBA/M.1.A.001.html", "discord-main", true)
	if result != "新增推文成功" {
		t.Fatalf("result = %q, want 新增推文成功", result)
	}
	if calls != 1 {
		t.Fatalf("PTT fetch calls = %d, want 1", calls)
	}
	member, err := commandRedis.IsMember("article:M.1.A.001:subs", "discord-main")
	if err != nil {
		t.Fatalf("read subscriber index: %v", err)
	}
	if !commandRedis.Exists("article:M.1.A.001:detail") || !member {
		t.Fatal("article detail or subscriber index was not persisted")
	}
}

func TestCheckArticleExistDistinguishesNotFoundFromTemporaryFailure(t *testing.T) {
	commandRedis.FlushAll()
	originalFetch := fetchArticleFromPTT
	defer func() { fetchArticleFromPTT = originalFetch }()

	fetchArticleFromPTT = func(context.Context, string, string) (article.Article, error) {
		return article.Article{}, web.URLNotFoundError{URL: "missing"}
	}
	exists, err := checkArticleExist(context.Background(), "NBA", "M.2.A.002")
	if err != nil || exists {
		t.Fatalf("404 result = (%v, %v), want (false, nil)", exists, err)
	}

	fetchArticleFromPTT = func(context.Context, string, string) (article.Article, error) {
		return article.Article{}, web.ErrBotChallenge
	}
	exists, err = checkArticleExist(context.Background(), "NBA", "M.3.A.003")
	if exists || !errors.Is(err, web.ErrBotChallenge) {
		t.Fatalf("challenge result = (%v, %v), want temporary error", exists, err)
	}
	if commandRedis.Exists("article:M.3.A.003:detail") || commandRedis.Exists("article:M.3.A.003:subs") {
		t.Fatal("temporary failure created article data")
	}
}

func TestCheckArticleExistPropagatesCancellation(t *testing.T) {
	commandRedis.FlushAll()
	originalFetch := fetchArticleFromPTT
	defer func() { fetchArticleFromPTT = originalFetch }()
	fetchArticleFromPTT = func(ctx context.Context, _, _ string) (article.Article, error) {
		<-ctx.Done()
		return article.Article{}, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := checkArticleExist(ctx, "NBA", "M.4.A.004")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestMultiBoardCommandCommitsOnceWithMatchingIndexes(t *testing.T) {
	commandRedis.FlushAll()
	saveCommandUser(t, "discord-main")
	seedCommandBoards(t, "stock", "nba")

	result := HandleCommandContext(context.Background(), "新增 stock,nba 台積電", "discord-main", true)
	if result != "新增成功" {
		t.Fatalf("result = %q, want 新增成功", result)
	}
	stored := models.User().Find("discord-main")
	if len(stored.Subscribes) != 2 {
		t.Fatalf("subscriptions = %#v, want two boards", stored.Subscribes)
	}
	for _, board := range []string{"stock", "nba"} {
		member, err := commandRedis.IsMember("keyword:"+board+":subs", "discord-main")
		if err != nil || !member {
			t.Fatalf("keyword index for %s = (%t, %v), want member", board, member, err)
		}
	}
}

func TestAuthorCommandStoresSubstringRuleAndDocumentsItsMeaning(t *testing.T) {
	commandRedis.FlushAll()
	saveCommandUser(t, "discord-main")
	seedCommandBoards(t, "stock")

	result := HandleCommandContext(context.Background(), "新增作者 stock AUTH", "discord-main", true)
	if result != "新增作者成功" {
		t.Fatalf("result = %q, want 新增作者成功", result)
	}
	stored := models.User().Find("discord-main")
	if len(stored.Subscribes) != 1 || len(stored.Subscribes[0].Authors) != 1 || stored.Subscribes[0].Authors[0] != "AUTH" {
		t.Fatalf("stored author rules = %#v, want AUTH preserved as a substring rule", stored.Subscribes)
	}
	if help := HandleCommandContext(context.Background(), "指令", "discord-main", true); !strings.Contains(help, "作者包含追蹤（不分大小寫）") {
		t.Fatalf("help = %q, want author substring semantics", help)
	}
}

func TestKeywordCommandStoresAndDocumentsAuthorAndTitleRule(t *testing.T) {
	commandRedis.FlushAll()
	saveCommandUser(t, "discord-main")
	seedCommandBoards(t, "hardwaresale")

	rule := "author:lushin&賣"
	result := HandleCommandContext(context.Background(), "新增 HardwareSale "+rule, "discord-main", true)
	if result != "新增成功" {
		t.Fatalf("result = %q, want 新增成功", result)
	}
	stored := models.User().Find("discord-main")
	if len(stored.Subscribes) != 1 || len(stored.Subscribes[0].Keywords) != 1 || stored.Subscribes[0].Keywords[0] != rule {
		t.Fatalf("stored keyword rules = %#v, want %q preserved", stored.Subscribes, rule)
	}
	if help := HandleCommandContext(context.Background(), "指令", "discord-main", true); !strings.Contains(help, "author:lushin&賣") {
		t.Fatalf("help = %q, want author-and-title example", help)
	}
}

func TestKeywordCommandsRejectMalformedAuthorAndTitleRules(t *testing.T) {
	commandRedis.FlushAll()
	saveCommandUser(t, "discord-main")
	seedCommandBoards(t, "hardwaresale")

	for _, text := range []string{
		"新增 HardwareSale author:&賣",
		"新增 HardwareSale author:lushin&",
		"新增 HardwareSale author:lushin&&賣",
		"add -k author:lushin& HardwareSale",
	} {
		result := ExecuteCommandContext(context.Background(), text, "discord-main", true)
		if result.Kind != ExecutionKindInvalid || !strings.Contains(result.Message, errInvalidAuthorTitleKeyword.Error()) {
			t.Errorf("command %q result = %#v, want understandable invalid-rule error", text, result)
		}
	}

	if stored := models.User().Find("discord-main"); len(stored.Subscribes) != 0 {
		t.Fatalf("malformed commands changed subscriptions: %#v", stored.Subscribes)
	}
}

func TestDeletingCompoundKeywordRetainsOtherKeywordIndexAndCursor(t *testing.T) {
	commandRedis.FlushAll()
	saveCommandUser(t, "discord-main")
	seedCommandBoards(t, "hardwaresale")

	const compound = "author:lushin&賣"
	if result := HandleCommandContext(
		context.Background(),
		"新增 HardwareSale "+compound+",ddr4",
		"discord-main",
		true,
	); result != "新增成功" {
		t.Fatalf("add result = %q, want 新增成功", result)
	}
	cursorBefore, err := commandRedis.Get("board:hardwaresale")
	if err != nil {
		t.Fatalf("read activation cursor: %v", err)
	}

	if result := HandleCommandContext(
		context.Background(),
		"刪除 HardwareSale "+compound,
		"discord-main",
		true,
	); result != "刪除成功" {
		t.Fatalf("delete result = %q, want 刪除成功", result)
	}
	stored := models.User().Find("discord-main")
	if len(stored.Subscribes) != 1 || len(stored.Subscribes[0].Keywords) != 1 ||
		stored.Subscribes[0].Keywords[0] != "ddr4" {
		t.Fatalf("subscriptions after exact delete = %#v, want only ddr4", stored.Subscribes)
	}
	member, err := commandRedis.IsMember("keyword:hardwaresale:subs", "discord-main")
	if err != nil || !member {
		t.Fatalf("keyword index after exact delete = (%t, %v), want retained", member, err)
	}
	cursorAfter, err := commandRedis.Get("board:hardwaresale")
	if err != nil {
		t.Fatalf("read cursor after delete: %v", err)
	}
	if cursorAfter != cursorBefore {
		t.Fatalf("cursor changed during keyword edit: before %q, after %q", cursorBefore, cursorAfter)
	}
}

func TestMultiBoardCommandPreflightFailureWritesNothing(t *testing.T) {
	commandRedis.FlushAll()
	saveCommandUser(t, "discord-main")
	seedCommandBoards(t, "stock", "nba")
	commandRedis.Set("keyword:nba:subs", "wrong-type")

	result := HandleCommandContext(context.Background(), "新增 stock,nba 台積電", "discord-main", true)
	if !strings.Contains(result, "失敗") {
		t.Fatalf("result = %q, want failure", result)
	}
	stored := models.User().Find("discord-main")
	if len(stored.Subscribes) != 0 {
		t.Fatalf("subscriptions changed despite failed transaction: %#v", stored.Subscribes)
	}
	if commandRedis.Exists("keyword:stock:subs") {
		t.Fatal("first board index was written before second board failed")
	}
}

func TestCommandLineFlagsCommitAsOneTransaction(t *testing.T) {
	commandRedis.FlushAll()
	saveCommandUser(t, "discord-main")
	seedCommandBoards(t, "stock", "nba")

	result := HandleCommandContext(context.Background(), "add -k chip -a author -p 20 stock nba", "discord-main", true)
	if result != "新增成功。" {
		t.Fatalf("result = %q, want 新增成功。", result)
	}
	for _, board := range []string{"stock", "nba"} {
		for _, key := range []string{"keyword:" + board + ":subs", "author:" + board + ":subs", "pushsum:" + board + ":subs"} {
			member, err := commandRedis.IsMember(key, "discord-main")
			if err != nil || !member {
				t.Fatalf("index %s = (%t, %v), want member", key, member, err)
			}
		}
	}
}

func TestCommandLineFlagsDoNotPartiallyCommit(t *testing.T) {
	commandRedis.FlushAll()
	saveCommandUser(t, "discord-main")
	seedCommandBoards(t, "stock", "nba")
	commandRedis.Set("author:nba:subs", "wrong-type")

	result := HandleCommandContext(context.Background(), "add -k chip -a author stock nba", "discord-main", true)
	if !strings.Contains(result, "失敗") {
		t.Fatalf("result = %q, want failure", result)
	}
	stored := models.User().Find("discord-main")
	if len(stored.Subscribes) != 0 {
		t.Fatalf("subscriptions partially committed: %#v", stored.Subscribes)
	}
	for _, key := range []string{"keyword:stock:subs", "keyword:nba:subs", "author:stock:subs"} {
		if commandRedis.Exists(key) {
			t.Fatalf("index %s was partially committed", key)
		}
	}
}

func TestCommandLineValidatesEachBoardOnlyOnce(t *testing.T) {
	commandRedis.FlushAll()
	saveCommandUser(t, "discord-main")
	originalVerify := verifyBoardExist
	defer func() { verifyBoardExist = originalVerify }()
	calls := make(map[string]int)
	verifyBoardExist = func(_ context.Context, boardName string) (bool, string, error) {
		calls[boardName]++
		return true, "", nil
	}

	result := HandleCommandContext(context.Background(), "add -k chip -a author -p 20 -b 10 stock nba", "discord-main", true)
	if result != "新增成功。" {
		t.Fatalf("result = %q, want 新增成功。", result)
	}
	for _, board := range []string{"stock", "nba"} {
		if calls[board] != 1 {
			t.Fatalf("board %s validation calls = %d, want 1", board, calls[board])
		}
	}
}

func TestWildcardCanOnlyRemoveSubscriptions(t *testing.T) {
	commandRedis.FlushAll()
	seedCommandBoards(t, "stock")
	saveCommandUser(t, "discord-main")

	for _, text := range []string{
		"新增 stock *",
		"新增作者 stock *",
		"add -k * stock",
		"add -a * stock",
	} {
		result := HandleCommandContext(context.Background(), text, "discord-main", true)
		if !strings.Contains(result, errWildcardAdd.Error()) {
			t.Errorf("command %q result = %q, want wildcard error", text, result)
		}
	}

	stored := models.User().Find("discord-main")
	if len(stored.Subscribes) != 0 {
		t.Fatalf("subscriptions = %#v, want empty", stored.Subscribes)
	}
	for _, key := range []string{"keyword:stock:subs", "author:stock:subs"} {
		if commandRedis.Exists(key) {
			t.Fatalf("unexpected Redis key %s", key)
		}
	}
}

func TestCommandLineWildcardRemovesAllKeywordsAndAuthors(t *testing.T) {
	commandRedis.FlushAll()
	seedCommandBoards(t, "stock")
	saveCommandUser(t, "discord-main")

	if result := HandleCommandContext(context.Background(), "add -k chip -a author stock", "discord-main", true); result != "新增成功。" {
		t.Fatalf("add result = %q", result)
	}
	if result := HandleCommandContext(context.Background(), "del -k * -a * stock", "discord-main", true); result != "刪除成功。" {
		t.Fatalf("delete result = %q", result)
	}
	if stored := models.User().Find("discord-main"); len(stored.Subscribes) != 0 {
		t.Fatalf("subscriptions = %#v, want empty", stored.Subscribes)
	}
}

func TestZeroPushThresholdDoesNotCreateEmptySubscription(t *testing.T) {
	commandRedis.FlushAll()
	seedCommandBoards(t, "stock")
	saveCommandUser(t, "discord-main")

	result := HandleCommandContext(context.Background(), "新增推文數 stock 0", "discord-main", true)
	if result != "新增推文數成功" {
		t.Fatalf("result = %q", result)
	}
	stored := models.User().Find("discord-main")
	if len(stored.Subscribes) != 0 {
		t.Fatalf("subscriptions = %#v, want empty", stored.Subscribes)
	}
}

func TestExecuteCommandContextReturnsStructuredKinds(t *testing.T) {
	commandRedis.FlushAll()
	saveCommandUser(t, "discord-main")
	seedCommandBoards(t, "stock")

	assertExecutionKind(t, ExecuteCommandContext(context.Background(), "not-a-command", "discord-main", true), ExecutionKindInvalid)
	assertExecutionKind(t, ExecuteCommandContext(context.Background(), "新增", "discord-main", true), ExecutionKindInvalid)

	originalVerify := verifyBoardExist
	defer func() { verifyBoardExist = originalVerify }()
	verifyBoardExist = func(context.Context, string) (bool, string, error) {
		return false, "", nil
	}
	missingBoard := ExecuteCommandContext(context.Background(), "新增 missing keyword", "discord-main", true)
	assertExecutionKind(t, missingBoard, ExecutionKindInvalid)
	if strings.Contains(missingBoard.Message, "可能板名") {
		t.Fatalf("empty suggestion leaked into message: %q", missingBoard.Message)
	}

	verifyBoardExist = func(context.Context, string) (bool, string, error) {
		return false, "", web.ErrBotChallenge
	}
	assertExecutionKind(t, ExecuteCommandContext(context.Background(), "新增 missing keyword", "discord-main", true), ExecutionKindPTTUnavailable)
	verifyBoardExist = originalVerify

	originalFetch := fetchArticleFromPTT
	defer func() { fetchArticleFromPTT = originalFetch }()
	fetchArticleFromPTT = func(context.Context, string, string) (article.Article, error) {
		return article.Article{}, web.URLNotFoundError{URL: "missing"}
	}
	assertExecutionKind(t, ExecuteCommandContext(
		context.Background(),
		"新增推文 https://www.ptt.cc/bbs/Stock/M.1.A.001.html",
		"discord-main",
		true,
	), ExecutionKindInvalid)
	fetchArticleFromPTT = originalFetch

	originalCommit := commitSubscriptionUpdate
	defer func() { commitSubscriptionUpdate = originalCommit }()
	commitSubscriptionUpdate = func(*user.User, user.User) error {
		return user.ErrConcurrentUpdate
	}
	assertExecutionKind(t, ExecuteCommandContext(context.Background(), "新增 stock conflict", "discord-main", true), ExecutionKindConflict)

	commitSubscriptionUpdate = func(*user.User, user.User) error {
		return errors.New("storage failed")
	}
	assertExecutionKind(t, ExecuteCommandContext(context.Background(), "新增 stock internal", "discord-main", true), ExecutionKindInternal)
	commitSubscriptionUpdate = originalCommit

	addResult := ExecuteCommandContext(context.Background(), "新增 stock 失敗", "discord-main", true)
	assertExecutionKind(t, addResult, ExecutionKindSuccess)
	listResult := ExecuteCommandContext(context.Background(), "清單", "discord-main", true)
	assertExecutionKind(t, listResult, ExecutionKindSuccess)
	if !strings.Contains(listResult.Message, "失敗") {
		t.Fatalf("list result = %q, want subscribed keyword", listResult.Message)
	}
}

func assertExecutionKind(t *testing.T, result ExecutionResult, want ExecutionKind) {
	t.Helper()
	if result.Kind != want {
		t.Fatalf("result = %#v, want kind %q", result, want)
	}
}
