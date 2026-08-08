package command

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	log "github.com/Ptt-Alertor/logrus"
	"github.com/Ptt-Alertor/ptt-alertor/models"
	"github.com/Ptt-Alertor/ptt-alertor/models/article"
	"github.com/Ptt-Alertor/ptt-alertor/models/board"
	"github.com/Ptt-Alertor/ptt-alertor/models/commentcursor"
	"github.com/Ptt-Alertor/ptt-alertor/models/subscription"
	"github.com/Ptt-Alertor/ptt-alertor/models/top"
	"github.com/Ptt-Alertor/ptt-alertor/models/user"
	"github.com/Ptt-Alertor/ptt-alertor/ptt/rss"
	"github.com/Ptt-Alertor/ptt-alertor/ptt/web"
)

const subArticlesLimit int = 50
const updateFailedMsg string = "失敗，請稍後再試並檢查服務紀錄。"
const subscriptionUpdateAttempts = 3

// ExecutionKind describes the machine-readable outcome of a command. Human
// response text is intentionally kept separate so HTTP callers never have to
// infer success or failure by searching translated messages.
type ExecutionKind string

const (
	ExecutionKindSuccess        ExecutionKind = "success"
	ExecutionKindInvalid        ExecutionKind = "invalid"
	ExecutionKindPTTUnavailable ExecutionKind = "ptt_unavailable"
	ExecutionKindConflict       ExecutionKind = "conflict"
	ExecutionKindInternal       ExecutionKind = "internal"
)

// ExecutionResult is the structured command result used by API adapters.
// HandleCommandContext remains available for legacy callers that only consume
// the human-readable message.
type ExecutionResult struct {
	Message string
	Kind    ExecutionKind
}

func successResult(message string) ExecutionResult {
	return ExecutionResult{Message: message, Kind: ExecutionKindSuccess}
}

func invalidResult(message string) ExecutionResult {
	return ExecutionResult{Message: message, Kind: ExecutionKindInvalid}
}

func unavailableResult(message string) ExecutionResult {
	return ExecutionResult{Message: message, Kind: ExecutionKindPTTUnavailable}
}

func conflictResult() ExecutionResult {
	return ExecutionResult{Message: "訂閱資料同時被更新，請重試。", Kind: ExecutionKindConflict}
}

func internalResult(message string) ExecutionResult {
	return ExecutionResult{Message: message, Kind: ExecutionKindInternal}
}

var inputErrorTips = []string{
	"指令格式錯誤。",
	"1. 需以空白分隔動作、板名、參數",
	"2. 板名欄位開頭與結尾不可有逗號",
	"3. 板名欄位間不允許空白字元。",
}

// Commands is commands documents
var Commands = map[string]map[string]string{
	"一般": {
		"指令": "可使用的指令清單",
		"清單": "設定的看板、關鍵字、作者",
		"排行": "前五名追蹤的關鍵字、作者",
	},
	"關鍵字相關": {
		"新增 看板 關鍵字": "新增追蹤關鍵字",
		"刪除 看板 關鍵字": "取消追蹤關鍵字",
		"作者 AND 標題": "新增 HardwareSale author:lushin&賣（author: 必須置首，作者與標題詞皆不可空白）",
		"範例":        "新增 gossiping,movie 金城武,結衣",
	},
	"作者相關": {
		"新增作者 看板 作者片段": "新增作者包含追蹤（不分大小寫）",
		"刪除作者 看板 作者":   "取消追蹤作者",
		"範例":           "新增作者 gossiping ffa,obov",
	},
	"推噓文數相關": {
		"新增(推/噓)文數 看板 總數": "通知推或噓文數",
		"範例":              "新增推文數 joke,beauty 10",
		"歸零即刪除":           "新增噓文數 joke 0",
	},
	"推文相關": {
		"新增推文 網址": "新增推文追蹤",
		"刪除推文 網址": "刪除推文追蹤",
		"範例":      "新增推文 https://www.ptt.cc/bbs/EZsoft/M.1497363598.A.74E.html",
	},
	"進階應用": {
		"參考連結": "https://pttalertor.dinolai.com/docs",
	},
}

var commandActionMap = map[string]updateAction{
	"新增":    addKeywords,
	"刪除":    removeKeywords,
	"新增作者":  addAuthors,
	"刪除作者":  removeAuthors,
	"新增推文":  addArticles,
	"刪除推文":  removeArticles,
	"新增推文數": updatePushUp,
	"新增噓文數": updatePushDown,
}

// HandleCommand handles command from chatbot
func HandleCommand(text string, userID string, isUser bool) string {
	return HandleCommandContext(context.Background(), text, userID, isUser)
}

// HandleCommandContext handles a command and propagates cancellation through
// every PTT lookup required to validate a subscription.
func HandleCommandContext(ctx context.Context, text string, userID string, isUser bool) string {
	return ExecuteCommandContext(ctx, text, userID, isUser).Message
}

// ExecuteCommandContext executes a command and returns both its human-readable
// response and a stable outcome kind for API status mapping.
func ExecuteCommandContext(ctx context.Context, text string, userID string, isUser bool) ExecutionResult {
	fields := strings.Fields(strings.TrimSpace(text))
	if len(fields) == 0 {
		return invalidResult("指令不可空白")
	}
	command := strings.ToLower(fields[0])
	if isUser {
		log.WithFields(log.Fields{
			"account": userID,
			"command": command,
		}).Info("Command Request")
	}
	switch command {
	case "debug":
		return successResult(handleDebug(userID))
	case "清單", "list":
		return successResult(handleList(userID))
	case "指令", "help":
		return successResult(stringCommands())
	case "排行", "ranking":
		return successResult(listTop())
	case "新增", "刪除":
		re := regexp.MustCompile("^(新增|刪除)\\s+([^,，][\\w-_,，\\.]*[^,，:\\s]):?\\s+(\\*|.*[^\\s])")
		if matched := re.MatchString(text); !matched {
			errorTips := inputErrorTips
			additionalTips := []string{
				"正確範例：",
				command + " gossiping,lol 問卦,爆卦",
			}
			errorTips = append(errorTips, additionalTips...)
			return invalidResult(strings.Join(errorTips, "\n"))
		}
		args := re.FindStringSubmatch(text)
		return handleKeyword(ctx, command, userID, args[2], args[3])
	case "新增作者", "刪除作者":
		re := regexp.MustCompile("^(新增作者|刪除作者)\\s+([^,，][\\w-_,，\\.]*[^,，:\\s]):?\\s+(\\*|[\\s,\\w]+)$")
		matched := re.MatchString(text)
		if !matched {
			errorTips := inputErrorTips
			additionalTips := []string{
				"4. 作者為半形英文與數字組成。",
				"正確範例：",
				command + " gossiping,lol ffaarr,obov",
			}
			errorTips = append(errorTips, additionalTips...)
			return invalidResult(strings.Join(errorTips, "\n"))
		}
		args := re.FindStringSubmatch(text)
		return handleAuthor(ctx, command, userID, args[2], args[3])
	case "新增推文數", "新增噓文數":
		re := regexp.MustCompile("^(新增推文數|新增噓文數)\\s+([^,，][\\w-_,，\\.]*[^,，:\\s]):?\\s+(100|[1-9][0-9]|[0-9])$")
		matched := re.MatchString(text)
		if !matched {
			errorTips := inputErrorTips
			additionalTips := []string{
				"4. 推噓文數需為介於 0-100 的數字",
				"正確範例：",
				command + " gossiping,beauty 100",
			}
			errorTips = append(errorTips, additionalTips...)
			return invalidResult(strings.Join(errorTips, "\n"))
		}
		args := re.FindStringSubmatch(text)
		return handlePushSum(ctx, command, userID, args[2], args[3])
	case "新增推文", "刪除推文":
		re := regexp.MustCompile("^(新增推文|刪除推文)\\s+https?://www.ptt.cc/bbs/([\\w-_]*)/(M\\.\\d+.A.\\w*)\\.html$")
		matched := re.MatchString(text)
		if !matched {
			errorTips := []string{
				"指令格式錯誤。",
				"1. 網址與指令需至少一個空白。",
				"2. 網址錯誤格式。",
				"正確範例：",
				command + " https://www.ptt.cc/bbs/EZsoft/M.1497363598.A.74E.html",
			}
			return invalidResult(strings.Join(errorTips, "\n"))
		}
		args := re.FindStringSubmatch(text)
		return handleComment(ctx, command, userID, args[2], args[3])
	case "清理推文":
		return cleanCommentList(ctx, userID)
	case "推文清單":
		return successResult(handleCommentList(userID))
	case "add", "del":
		return handleCommandLine(ctx, userID, command, text)
	}
	if !isUser {
		return successResult("")
	}
	return invalidResult("無此指令，請打「指令」查看指令清單")
}

func handleCommandLine(ctx context.Context, userID, command, text string) ExecutionResult {
	var keywordStr, authorStr, push, boo string
	cl := flag.NewFlagSet("Ptt Alertor: <add|del> <-flag <argument>> <board> [board...]\nexample: add -k ptt -a chodino -p 10 ezsoft", flag.ContinueOnError)
	bf := new(bytes.Buffer)
	cl.SetOutput(bf)
	cl.StringVar(&keywordStr, "keyword", "", "keywords: <keyword>[,keyword...]")
	cl.StringVar(&keywordStr, "k", "", "abbr. of keyword")
	cl.StringVar(&authorStr, "author", "", "authors: <author>[,author...]")
	cl.StringVar(&authorStr, "a", "", "abbr. of author")
	cl.StringVar(&push, "push", "", "number of push's sum: <sum>")
	cl.StringVar(&push, "p", "", "abbr. of push")
	cl.StringVar(&boo, "boo", "", "number of boo's sum: <sum>")
	cl.StringVar(&boo, "b", "", "abbr. of boo")

	args := strings.Fields(text)
	parseErr := cl.Parse(args[1:])
	boardStrs := cl.Args()
	for i := 0; i < len(boardStrs); i++ {
		boardStrs[i] = strings.TrimSpace(strings.Trim(boardStrs[i], ","))
	}
	boardStr := strings.Join(boardStrs, ",")
	if bf.Len() != 0 {
		if errors.Is(parseErr, flag.ErrHelp) {
			return successResult(bf.String())
		}
		return invalidResult(bf.String())
	}
	if parseErr != nil {
		return invalidResult(parseErr.Error())
	}
	if cl.NFlag() == 0 {
		return invalidResult("未指定參數。輸入 " + command + " -h 查看參數列表。")
	}
	if boardStr == "" {
		errorTips := []string{
			"未指定板名。",
			"範例：add -k ptt -a chodino -p 10 ezsoft",
			"輸入 " + command + " -h 查看提示訊息。",
		}
		return invalidResult(strings.Join(errorTips, "\n"))
	}

	log.WithField("command", text).Info("Command Line Request")

	var commandPrefix string
	switch command {
	case "add":
		commandPrefix = "新增"
	case "del":
		commandPrefix = "刪除"
	}

	boardNames := splitParamString(boardStr)
	mutations := make([]subscriptionMutation, 0, 4)
	if keywordStr != "" {
		if commandPrefix == "新增" && containsWildcard([]string{keywordStr}) {
			return invalidResult(errWildcardAdd.Error())
		}
		var inputs []string
		if strings.HasPrefix(keywordStr, "regexp:") {
			if !checkRegexp(keywordStr) {
				return invalidResult("正規表示式錯誤，請檢查規則。")
			}
			inputs = []string{keywordStr}
		} else {
			inputs = splitParamString(keywordStr)
		}
		mutations = append(mutations, subscriptionMutation{
			action: commandActionMap[commandPrefix], boardNames: boardNames, inputs: inputs,
		})
	}
	if authorStr != "" {
		if commandPrefix == "新增" && containsWildcard([]string{authorStr}) {
			return invalidResult(errWildcardAdd.Error())
		}
		if ok, _ := regexp.MatchString("^(\\*|[\\s,\\w]+)$", authorStr); !ok {
			return invalidResult("作者為半形英文與數字組成。")
		}
		mutations = append(mutations, subscriptionMutation{
			action: commandActionMap[commandPrefix+"作者"], boardNames: boardNames, inputs: splitParamString(authorStr),
		})
	}
	if push != "" {
		if commandPrefix == "刪除" {
			push = "0"
		}
		if err := validatePushSumInput(boardNames, push); err != nil {
			return invalidResult(err.Error())
		}
		mutations = append(mutations, subscriptionMutation{
			action: updatePushUp, boardNames: boardNames, inputs: []string{push},
		})
	}
	if boo != "" {
		if commandPrefix == "刪除" {
			boo = "0"
		}
		if err := validatePushSumInput(boardNames, boo); err != nil {
			return invalidResult(err.Error())
		}
		mutations = append(mutations, subscriptionMutation{
			action: updatePushDown, boardNames: boardNames, inputs: []string{boo},
		})
	}
	if err := updateBatch(ctx, userID, mutations); err != nil {
		return mutationFailureResult(commandPrefix, "Atomic Command Line Update Failed", err)
	}
	return successResult(commandPrefix + "成功。")
}

func validatePushSumInput(boardNames []string, value string) error {
	sum, err := strconv.Atoi(value)
	if err != nil || sum < 0 || sum > 100 {
		return errors.New("推噓文數需為介於 0-100 的數字")
	}
	for _, boardName := range boardNames {
		if strings.EqualFold(boardName, "allpost") {
			return errors.New("推文數通知不支持 ALLPOST 板。")
		}
	}
	return nil
}

func handleDebug(account string) string {
	return models.User().Find(account).Profile.Account
}

func handleList(account string) string {
	subs := models.User().Find(account).Subscribes
	if len(subs) == 0 {
		return "尚未建立清單。請打「指令」查看新增方法。"
	}
	return subs.String()
}

func cleanCommentList(ctx context.Context, account string) ExecutionResult {
	var i int
	for _, sub := range models.User().Find(account).Subscribes {
		for _, code := range sub.Articles {
			article := models.Article()
			article.Code = code
			bl, err := article.Exist()
			if err != nil {
				log.WithError(err).Error("Clean Comment List Failed")
				return internalResult("清理推文" + updateFailedMsg)
			}
			if !bl {
				if err := update(ctx, removeArticles, account, []string{sub.Board}, code); err != nil {
					return mutationFailureResult("清理推文", "Clean Comment List Update Failed", err)
				}
				i++
			}
		}
	}
	return successResult(fmt.Sprintf("清理 %d 則推文", i))
}

func handleCommentList(account string) string {
	subs := models.User().Find(account).Subscribes
	if len(subs) == 0 {
		return "尚未建立清單。請打「指令」查看新增方法。"
	}
	return "推文追蹤清單，上限 50 篇：\n" + subs.StringCommentList() + "\n輸入「清理推文」，可刪除無效連結。"
}

func stringCommands() string {
	str := ""
	for cat, cmds := range Commands {
		str += "[" + cat + "]\n"
		for cmd, doc := range cmds {
			str += cmd
			if doc != "" {
				str += "：" + doc
			}
			str += "\n"
		}
		str += "\n"
	}
	return strings.TrimSpace(str)
}

func listTop() string {
	content := "關鍵字"
	for i, keyword := range top.ListKeywords(5) {
		content += fmt.Sprintf("\n%d. %s", i+1, keyword)
	}
	content += "\n----\n作者"
	for i, author := range top.ListAuthors(5) {
		content += fmt.Sprintf("\n%d. %s", i+1, author)
	}
	content += "\n----\n推噓文"
	for i, pushSum := range top.ListPushSum(5) {
		content += fmt.Sprintf("\n%d. %s", i+1, pushSum)
	}
	content += "\n\nTOP 100:\nhttp://pttalertor.dinolai.com/top"
	return content
}

func handleKeyword(ctx context.Context, command, userID, board, keywordStr string) ExecutionResult {
	boardNames := splitParamString(board)
	input := keywordStr
	var inputs []string
	if strings.HasPrefix(input, "regexp:") {
		if !checkRegexp(input) {
			return invalidResult("正規表示式錯誤，請檢查規則。")
		}
		inputs = []string{keywordStr}
	} else {
		inputs = splitParamString(keywordStr)
	}
	log.WithFields(log.Fields{
		"id":      userID,
		"command": command,
		"boards":  boardNames,
		"words":   inputs,
	}).Info("Keyword Command")
	err := update(ctx, commandActionMap[command], userID, boardNames, inputs...)
	if err != nil {
		return mutationFailureResult(command, "Keyword Command Failed", err)
	}
	return successResult(command + "成功")
}

func handleAuthor(ctx context.Context, command, userID, board, authorStr string) ExecutionResult {
	if ok, _ := regexp.MatchString("^(\\*|[\\s,\\w]+)$", authorStr); !ok {
		return invalidResult("作者為半形英文與數字組成。")
	}
	boardNames := splitParamString(board)
	authors := splitParamString(authorStr)
	log.WithFields(log.Fields{
		"id":      userID,
		"command": command,
		"boards":  boardNames,
		"words":   authors,
	}).Info("Author Command")
	err := update(ctx, commandActionMap[command], userID, boardNames, authors...)
	if err != nil {
		return mutationFailureResult(command, "Author Command Failed", err)
	}
	return successResult(command + "成功")
}

func handlePushSum(ctx context.Context, command, account, board, sumStr string) ExecutionResult {
	if sum, err := strconv.Atoi(sumStr); err != nil || sum < 0 || sum > 100 {
		return invalidResult("推噓文數需為介於 0-100 的數字")
	}
	boardNames := splitParamString(board)
	log.WithFields(log.Fields{
		"id":      account,
		"command": command,
		"boards":  boardNames,
		"words":   sumStr,
	}).Info("PushSum Command")
	for _, boardName := range boardNames {
		if strings.EqualFold(boardName, "allpost") {
			return invalidResult("推文數通知不支持 ALLPOST 板。")
		}
	}
	err := update(ctx, commandActionMap[command], account, boardNames, sumStr)
	if err != nil {
		return mutationFailureResult(command, "PushSum Command Failed", err)
	}
	return successResult(command + "成功")
}

func handleComment(ctx context.Context, command, userID, boardName, articleCode string) ExecutionResult {
	log.WithFields(log.Fields{
		"id":      userID,
		"command": command,
		"boards":  boardName,
		"words":   articleCode,
	}).Info("Comment Command")
	if strings.EqualFold(command, "新增推文") {
		exists, err := checkArticleExist(ctx, boardName, articleCode)
		if err != nil {
			if errors.Is(err, errArticleStorage) {
				log.WithError(err).Error("Article Storage Check Failed")
				return internalResult("文章資料處理失敗，請稍後再試並檢查服務紀錄。")
			}
			if result, ok := classifyExecutionError(err); ok {
				return result
			}
			var networkError net.Error
			if errors.As(err, &networkError) {
				return unavailableResult("PTT 暫時無法連線或正在限制請求，請稍後再試。")
			}
			log.WithError(err).Error("Article Check Failed")
			return internalResult("文章資料處理失敗，請稍後再試並檢查服務紀錄。")
		}
		if !exists {
			return invalidResult("文章不存在")
		}
		if countUserArticles(userID) >= subArticlesLimit {
			return invalidResult(errArticleSubscriptionLimit.Error())
		}
	}
	err := update(ctx, commandActionMap[command], userID, []string{boardName}, articleCode)
	if err != nil {
		return mutationFailureResult(command, "Comment Command Failed", err)
	}
	return successResult(command + "成功")
}

func countUserArticles(account string) (cnt int) {
	for _, sub := range models.User().Find(account).Subscribes {
		cnt += len(sub.Articles)
	}
	return cnt
}

var fetchArticleFromPTT = web.FetchArticleContext
var errArticleStorage = errors.New("article storage unavailable")
var resetInitialCommentCursor = func(code string, state commentcursor.State) error {
	return commentcursor.NewRedisStore().Reset(code, state)
}

func checkArticleExist(ctx context.Context, boardName, articleCode string) (bool, error) {
	a := models.Article()
	a.Code = articleCode
	bl, err := a.Exist()
	if err != nil {
		return false, fmt.Errorf("%w: %v", errArticleStorage, err)
	}
	if bl {
		return true, nil
	}
	atcl, err := fetchArticleFromPTT(ctx, boardName, articleCode)
	if err != nil {
		var notFound web.URLNotFoundError
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, err
	}
	if err := initialArticle(a, atcl); err != nil {
		return false, fmt.Errorf("%w: %v", errArticleStorage, err)
	}
	return true, nil
}

func initialArticle(a *article.Article, atcl article.Article) error {
	state, err := commentcursor.NewState(atcl.Comments)
	if err != nil {
		return err
	}
	a.Board = atcl.Board
	a.Code = atcl.Code
	a.Link = atcl.Link
	a.Title = atcl.Title
	a.ID = atcl.ID
	a.LastPushDateTime = atcl.LastPushDateTime
	a.Comments = atcl.Comments
	if err := a.Save(); err != nil {
		return err
	}
	return resetInitialCommentCursor(a.Code, state)
}

func mutationFailureResult(command, logMessage string, err error) ExecutionResult {
	if result, ok := classifyExecutionError(err); ok {
		return result
	}
	log.WithError(err).Error(logMessage)
	return internalResult(command + updateFailedMsg)
}

func classifyExecutionError(err error) (ExecutionResult, bool) {
	if err == nil {
		return ExecutionResult{}, false
	}
	if errors.Is(err, errWildcardAdd) || errors.Is(err, errInvalidAuthorTitleKeyword) ||
		errors.Is(err, errArticleSubscriptionLimit) {
		return invalidResult(err.Error()), true
	}
	if errors.Is(err, user.ErrConcurrentUpdate) || errors.Is(err, user.ErrUserNotExist) {
		return conflictResult(), true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, web.ErrBotChallenge) || errors.Is(err, web.ErrUnexpectedRedirect) {
		return unavailableResult("PTT 暫時無法連線或正在限制請求，請稍後再試。"), true
	}
	var statusErr web.HTTPStatusError
	if errors.As(err, &statusErr) && (statusErr.StatusCode == http.StatusForbidden ||
		statusErr.StatusCode == http.StatusTooManyRequests || statusErr.StatusCode >= http.StatusInternalServerError) {
		return unavailableResult("PTT 暫時無法連線或正在限制請求，請稍後再試。"), true
	}
	if errors.Is(err, rss.ErrTemporarilyUnavailable) || errors.Is(err, rss.ErrTooManyRequests) {
		return unavailableResult("PTT 暫時無法連線或正在限制請求，請稍後再試。"), true
	}
	if errors.Is(err, web.ErrOver18ConsentRequired) {
		return invalidResult("限制級看板需要先由操作者確認已滿 18 歲並設定 PTT_OVER18=true。"), true
	}
	var boardErr board.BoardNotExistError
	if errors.As(err, &boardErr) {
		message := "板名錯誤，請確認拼字。"
		if suggestion := strings.TrimSpace(boardErr.Suggestion); suggestion != "" {
			message += "可能板名：\n" + suggestion
		}
		return invalidResult(message), true
	}
	return ExecutionResult{}, false
}

func checkRegexp(input string) bool {
	pattern := strings.Replace(strings.TrimPrefix(input, "regexp:"), "//", "////", -1)
	_, err := regexp.Compile(pattern)
	if err != nil {
		return false
	}
	return true
}

func splitParamString(paramString string) (params []string) {
	paramString = strings.Trim(paramString, ",，")
	if !strings.ContainsAny(paramString, ",，") {
		return []string{paramString}
	}

	if strings.Contains(paramString, ",") {
		params = strings.Split(paramString, ",")
	} else {
		params = []string{paramString}
	}

	for i := 0; i < len(params); i++ {
		if strings.Contains(params[i], "，") {
			params = append(params[:i], append(strings.Split(params[i], "，"), params[i+1:]...)...)
			i--
		}
	}

	for i, param := range params {
		params[i] = strings.TrimSpace(param)
	}

	return params
}

func update(ctx context.Context, action updateAction, account string, boardNames []string, inputs ...string) error {
	return updateBatch(ctx, account, []subscriptionMutation{{
		action: action, boardNames: boardNames, inputs: inputs,
	}})
}

type subscriptionMutation struct {
	action     updateAction
	boardNames []string
	inputs     []string
}

var commitSubscriptionUpdate = func(current *user.User, previous user.User) error {
	return current.UpdateSubscriptions(previous)
}

func updateBatch(ctx context.Context, account string, mutations []subscriptionMutation) error {
	if len(mutations) == 0 {
		return nil
	}
	actionCtx := context.WithValue(ctx, boardValidationCacheContextKey{}, make(boardValidationCache))
	for attempt := 0; attempt < subscriptionUpdateAttempts; attempt++ {
		u := models.User().Find(account)
		if u.Profile.Account == "" {
			return user.ErrUserNotExist
		}
		previous := u.Clone()
		changed := false
		for _, mutation := range mutations {
			if len(mutation.boardNames) == 0 {
				continue
			}
			targetBoards := append([]string(nil), mutation.boardNames...)
			if targetBoards[0] == "**" {
				targetBoards = nil
				for _, uSub := range u.Subscribes {
					targetBoards = append(targetBoards, uSub.Board)
				}
			}
			for _, boardName := range targetBoards {
				if err := actionCtx.Err(); err != nil {
					return err
				}
				sub := subscription.Subscription{Board: strings.ToLower(boardName)}
				if err := mutation.action(actionCtx, &u, sub, mutation.inputs...); err != nil {
					return err
				}
				changed = true
			}
		}
		if !changed {
			return nil
		}
		err := commitSubscriptionUpdate(&u, previous)
		if !errors.Is(err, user.ErrConcurrentUpdate) {
			if err != nil {
				log.WithError(err).Error("Atomic Subscription Update Error")
			}
			return err
		}
	}
	return user.ErrConcurrentUpdate
}

func HandleLineFollow(id, accountType string) error {
	u := models.User().Find(id)
	u.Profile.Line, u.Profile.Type = id, accountType
	log.WithFields(log.Fields{
		"id":       id,
		"type":     accountType,
		"platform": "line",
	}).Info("User Join")
	return handleFollow(u)
}

func HandleMessengerFollow(id string) error {
	u := models.User().Find(id)
	u.Profile.Messenger = id
	log.WithFields(log.Fields{
		"id":       id,
		"platform": "messenger",
	}).Info("User Join")
	return handleFollow(u)
}

func HandleTelegramFollow(id string, chatID int64) error {
	u := models.User().Find(id)
	u.Profile.Telegram = id
	u.Profile.TelegramChat = chatID
	log.WithFields(log.Fields{
		"id":       id,
		"platform": "telegram",
	}).Info("User Join")
	return handleFollow(u)
}

func handleFollow(u user.User) error {
	if u.Profile.Account != "" {
		u.Enable = true
		u.Update()
	} else {
		if u.Profile.Messenger != "" {
			u.Profile.Account = u.Profile.Messenger
		}
		if u.Profile.Line != "" {
			u.Profile.Account = u.Profile.Line
		}
		if u.Profile.Telegram != "" {
			u.Profile.Account = u.Profile.Telegram
		}
		u.Enable = true
		err := u.Save()
		if err != nil {
			return err
		}
	}
	return nil
}
