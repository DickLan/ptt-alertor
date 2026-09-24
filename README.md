# Ptt Alertor

Ptt 看板文章、作者、關鍵字、推噓文數與推文追蹤器。這個維護版的對外通知已統一改為 **Discord webhook**；LINE、Messenger、Telegram 與 email 不再由主程式啟動或發送。

## 快速啟動

需求：Docker Engine 與 Docker Compose v2.24 以上（使用 optional `env_file.required`）。

```bash
cd /opt/ptt-alertor
install -m 600 .env.example .env
```

至少修改 `.env` 內這些項目；管理密碼可用 `openssl rand -hex 32` 產生。Webhook 可以稍後再補，服務仍能先啟動，但會自動暫停背景輪詢。補上 Webhook 後要用 `docker compose up -d --force-recreate app` 重新建立 App（單純 `docker compose restart` 不會重新載入 `.env`）；Redis 中既有的 outbox backlog 會先繼續投遞：

```dotenv
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
AUTH_USER=admin
AUTH_PW=貼上產生的十六進位密碼
JOBS_ENABLED=false
PTT_COMMENT_JOBS_ENABLED=false
PTT_PUSHSUM_JOBS_ENABLED=false
```

啟動並確認 App、Redis 都健康：

```bash
docker compose up -d --build
docker compose ps
curl -fsS http://127.0.0.1:9090/healthz
curl -fsS http://127.0.0.1:9090/readyz
```

`healthz` 是程式存活檢查；`readyz` 會確認 Redis，且在 `JOBS_ENABLED=true` 時要求 Discord webhook 設定有效。查看執行紀錄：

```bash
docker compose logs -f app
```

若 Redis log 警告 `vm.overcommit_memory`，請由主機管理者評估後在 Docker host 設為 `1`（例如寫入 `/etc/sysctl.d/99-redis.conf` 後套用）。這是主機層 kernel 設定，Compose 不會擅自修改；忽略警告可能讓 Redis 在低記憶體或背景持久化時失敗。

官方 Compose 固定只將 HTTP 服務發布在 `127.0.0.1:9090`，外層 shell 不能改成全介面。管理 API 使用 HTTP Basic；需要從其他主機存取時，請保留 App 的本機綁定並在前方配置 HTTPS reverse proxy 與防火牆，不要直接把 9090 暴露到 LAN 或公網。若確實需要不同 bind policy，請用一份明確受審的 Compose override，而不是修改安全預設。

## 建立第一個 Discord 訂閱

Discord webhook 只有出站功能，因此使用驗證 API 管理訂閱。先建立一個啟用 Discord 的帳號，`subscribes` 必須先留空：

```bash
curl -u admin \
  -H 'Content-Type: application/json' \
  -d '{
    "enable": true,
    "profile": {"account": "discord-main", "discord": true},
    "subscribes": []
  }' \
  http://127.0.0.1:9090/users
```

再透過 command API 建立索引與訂閱。例如追蹤 Stock 板標題中的「台積電」：

```bash
curl -u admin \
  -H 'Content-Type: application/json' \
  -d '{"command":"新增 Stock 台積電"}' \
  http://127.0.0.1:9090/users/discord-main/commands
```

常用命令：

- `新增 Stock 台積電,聯發科`：任一關鍵字命中即通知。
- `新增 Stock 台積電&法說`：兩個關鍵字都命中才通知。
- `新增 HardwareSale author:lushin&賣`：同一篇文章的作者包含 `lushin`，且標題包含「賣」才通知；作者與標題皆不分大小寫。v1 規則必須以小寫 `author:` 開頭，作者和其後每個 `&` 標題詞都不可空白，且 `&` 兩側不要加空格；`賣&author:lushin` 不會被解讀為作者條件。
- `新增 Stock regexp:.*`：追蹤該板全部新文章。
- `新增作者 Stock auth`：作者欄位包含 `auth` 時通知；不分大小寫，例如也會命中 `SomeAuthor`。
- `新增推文數 Stock 50`、`新增噓文數 Stock 20`：追蹤推噓文數。
- `新增推文 https://www.ptt.cc/bbs/Stock/M....html`：追蹤單篇推文。
- `刪除 Stock 台積電`、`清單`、`指令`：刪除、檢視與列出說明。

訂閱必須走 command API；直接用 `PUT /users/:account` 改寫 `subscribes` 會被忽略，以免 Redis 的看板/關鍵字索引不同步。

## 測試 Discord webhook

新部署先保持 `JOBS_ENABLED=false`，填入 webhook，明確重新建立 App 並確認本地設定與 Redis 就緒：

```bash
docker compose up -d --force-recreate app
docker compose ps
curl -fsS http://127.0.0.1:9090/readyz
docker compose logs --tail=50 app
```

若這個 Redis volume 曾運行過本服務，有效 webhook 設定載入後會先續送既有 backlog；這與 `JOBS_ENABLED` 無關。可先查看 ready 與 leased 數量，兩者相加就是待送事件數：

```bash
docker compose exec redis redis-cli ZCARD ptta:discord:outbox:ready
docker compose exec redis redis-cli ZCARD ptta:discord:outbox:leased
```

再用管理廣播做最小送達測試：

```bash
curl -u admin \
  -H 'Content-Type: application/json' \
  -d '{"platforms":["discord"],"content":"Ptt Alertor Discord 測試成功"}' \
  http://127.0.0.1:9090/broadcast
```

上述 `curl -u admin` 會由 curl 互動詢問密碼，避免真實密碼出現在 shell history。親眼確認 Discord 收到測試訊息後，才把 `.env` 改成 `JOBS_ENABLED=true` 並執行 `docker compose up -d --force-recreate app`。再檢查 `readyz` 與 App logs；`readyz` 只驗證 Redis 和 webhook 的本地格式（HTTPS、Discord host 與 `/api/webhooks/<id>/<token>` 路徑），真實權限/撤銷狀態仍以廣播送達及投遞 log 為準。這可避免格式看似正確、但實際已失效或無權限的 webhook 在背景輪詢開始後才被發現。

若這次是在替換已撤銷的 webhook，舊 endpoint 的共用退避 TTL 可能仍在。只有在上述廣播已由新 webhook 確認送達後，才可用 `docker compose exec redis redis-cli DEL ptta:discord:outbox:webhook-not-before` 清除舊退避並立即續送；不清除也不會遺失資料，只是最久可能再等待 6 小時。

Compose 的 Webhook URL 與 `APP_HOST` 只從權限設為 `0600` 的 `.env` 讀取，不會被外層 shell 的同名變數意外覆蓋；Webhook 也不會存入使用者資料或寫入 log。正式 URL 必須使用 HTTPS（測試用 loopback 例外）；程式會強制 `wait=true`、禁止跟隨 3xx，只有 Discord 回傳已建立訊息的 `id` 才視為成功。訊息會依 Discord 的 2,000 UTF-16 code-unit 限制安全分段、停用 `allowed_mentions`，並針對 429/5xx 遵守等待時間後重試。

背景通知使用 Redis durable outbox：文章、留言或推噓門檻事件會先以完整來源 identity 寫入不可變分段，成功後才推進 PTT cursor。Worker 每確認一個 Discord message ID 才記錄該段進度；重啟後從未確認的段落續送。單一自動通知最多保留 4 段，異常長內容會附註後截斷；outbox 本身另以 8 段、每段 2,000 UTF-16 units 與總 byte 數做硬上限，避免一筆事件展開成無界 webhook 流量。Discord 回覆 429，或 webhook 明確不可用（401/403/404/410、未設定或格式無效）時，該次退避期限會原子保存成整個 webhook backlog 共用的 not-before，其他 ready event 不會提前重撞相同的 rate-limit bucket 或失效 endpoint。留言追蹤另會先保存不可變 transition（完整 occurrence snapshot、隨機 tracking epoch 與下一狀態），並用 Redis full-state CAS stage/advance/commit；並行輪詢或重新訂閱不能讓舊 epoch 覆寫新 lifecycle。相同事件在 pending 與完成後 30 天內具冪等性，預設 backlog 達 10,000 筆時暫停 PTT notification producers。投遞語意是 **at-least-once**：若 Discord 已收下訊息、但程式在 Redis ack 前中止，恢復後可能重送該段，這比靜默漏訊息安全。

## PTT 防爬與保守存取

本專案歷史上確實收到過 HTTP 429，也曾因高頻輪詢被 PTT 阻擋；但 PTT 沒有公開 2026 年現行門檻，也沒有證據能宣稱目前仍由 Cloudflare 防護。因此本版採保守而不繞過站方限制的策略：

- 全程共用一個 PTT request limiter，預設每秒最多啟動一個 request，且同時最多一個 in-flight request。
- Redis 共用總量預算預設每小時 900、每日 10,000 個 request；達上限就不連線，留待下一個 UTC window。
- 429 優先遵守 `Retry-After`，否則從 30 秒開始全域指數退避與 jitter，最高 15 分鐘。
- 403 視為可能封鎖/挑戰，預設全域冷卻 15 分鐘，不做代理輪換、UA 冒充或 CAPTCHA 繞過。
- `PTT_CIRCUIT_BREAKER_ENABLED=true`（預設）時，精確滑動 24 小時視窗內累計 3 次 403/429/challenge/reset 後會開啟 circuit breaker；cooldown 預設 24 小時，可設定的安全下限為 1 小時。事件記錄、計數與 cooldown 以一個 Redis 原子操作提交，重啟不會清除。診斷時可明確設為 `false` 停用這個重複訊號升級，但每次訊號的短 cooldown、全域 request interval、預算與 retry 規則仍然生效；只有下面獨立的 transport reset 開關可再停用 reset/EOF 的短 cooldown。若 Redis 寫入失敗則 fail-closed，不會冒險繼續連 PTT。
- 對端主動 reset、header 前 EOF 或 unexpected EOF 時不在同輪重試，預設按疑似封鎖冷卻 15 分鐘；已觀察到的 block signal 使用獨立 5 秒 bounded context 寫入 Redis，不會因原 request 同時取消而遺失。診斷時可明確設 `PTT_TRANSPORT_RESET_COOLDOWN_ENABLED=false`：reset/EOF 仍不重試，但不再記錄為 block 或套用 15 分鐘 cooldown，下一次只受全域 request interval、producer cycle 與 request budget 控制。這不會弱化明確 403、429 或 HTML challenge 的 cooldown。其他 500/502/503/504、timeout 與連線錯誤短暫退避，404 不重試。
- Atom 遇到 403/429/5xx 時不立刻改抓 HTML，避免失敗時流量加倍。其他 Atom 錯誤才嘗試 HTML；結構完整的 HTML 成功時將該板快取為 HTML-only 6 小時，兩者都失敗時持久退避 15 分鐘，避免每輪雙重請求。並行成功請求不會清掉另一請求剛建立的 backoff。
- 使用可辨識的誠實 User-Agent，不再假冒 Firefox。
- 看板第一次出現有效訂閱時，會與 user/index 同一個 Redis transaction 寫入訂閱啟用時間 watermark；首次輪詢只把啟用後的文章視為新文。即使 limiter 讓第一輪晚數分鐘，也不會再把這段期間的新文吞進靜默 baseline。
- Atom 的 20 篇視窗若已容納不下斷線期間的新文，會以完整文章代碼向舊頁有限回溯；找不到邊界或達頁數上限時保留 cursor，不把缺口當成成功。

Limiter 是單一 App process 內的保護；Compose 請維持一個 App replica。直接水平擴充會讓合計 PTT 流量倍增，多副本部署必須先另做共享 limiter 與 polling leader election。

詳細證據、現況驗證與操作原則見 [docs/PTT_ACCESS.md](docs/PTT_ACCESS.md)。

相關環境變數：

```dotenv
PTT_REQUEST_INTERVAL=1s
PTT_MAX_REQUESTS_PER_HOUR=900
PTT_MAX_REQUESTS_PER_DAY=10000
PTT_CIRCUIT_BREAKER_ENABLED=true
PTT_CIRCUIT_BREAKER_THRESHOLD=3
PTT_CIRCUIT_BREAKER_COOLDOWN=24h
PTT_TRANSPORT_RESET_COOLDOWN_ENABLED=true
PTT_COMMENT_JOBS_ENABLED=false
PTT_PUSHSUM_JOBS_ENABLED=false
PTT_HIGH_BOARD_CYCLE_INTERVAL=1m
PTT_BOARD_CYCLE_INTERVAL=1m
PTT_DELAYED_ARTICLE_BATCH_SPAN=10m
PTT_COMMENT_CYCLE_INTERVAL=10m
PTT_PUSHSUM_CYCLE_INTERVAL=15m
PTT_PUSHSUM_MAX_PAGES=20
PTT_CATCHUP_MAX_PAGES=20
PTT_REQUEST_TIMEOUT=30s
PTT_API_TIMEOUT=45s
PTT_MAX_RETRIES=3
PTT_RETRY_BASE_DELAY=30s
PTT_SERVER_RETRY_BASE_DELAY=2s
PTT_FORBIDDEN_COOLDOWN=15m
PTT_USER_AGENT=Ptt-Alertor/5.0 (+https://github.com/Ptt-Alertor/ptt-alertor)
DISCORD_OUTBOX_MAX_PENDING=10000
DISCORD_OUTBOX_SHUTDOWN_DRAIN=15s
```

限制級看板只有在操作者確認已滿 18 歲後，才能設定 `PTT_OVER18=true`。程式不會在未明確同意時自行加入 `over18=1` cookie。

## 執行與資料

- 預設 `STORAGE_BACKEND=redis`，所有必要資料都由 Compose 的持久化 Redis volume 保存，不再要求 AWS DynamoDB。
- 舊 DynamoDB driver 仍保留供既有部署遷移；新安裝建議使用 Redis。
- `BOARD_HIGH` 可填逗號分隔的優先看板；每輪只會輪詢其中仍有有效訂閱者的看板，空白或未訂閱項目都不會產生請求或 CPU busy loop。
- 留言 crawler 只在 `#main-content` 與所有推文欄位/時間都完整可解析時提交；已追蹤非空頁若暫時解析成空頁，會保留 cursor 而不把整篇歷史留言當成新留言重送。
- Atom 與 HTML response 都會讀取上限後再多檢查 1 byte；Atom 超過 2 MiB、HTML 超過 8 MiB 直接失敗，不會把截斷內容當成完整資料。看板頁也必須有唯一 main/action/list/paging 結構，任何文章 title/meta 缺漏都保留原 cursor 並進入既定 backoff，而不是 panic 或提交部分資料。
- 推爆統計只會在完整追溯到 48 小時邊界或第 1 頁後提交狀態；觸及 `PTT_PUSHSUM_MAX_PAGES` 時會保留舊狀態重試。高流量看板可調高，但會增加 PTT request 數。
- `JOBS_ENABLED` 只有明確設為 `true` 才會啟動 PTT 背景輪詢；空值、拼錯與其他值都維持 HTTP/API-only。新部署應先以 `false` 驗證 Discord 廣播，確認送達後再啟用背景工作。
- `JOBS_ENABLED=true` 時，看板文章 checker 一定啟動，負責目前主要的「新文章標題關鍵字／作者」通知。留言與推噓統計的程式碼和訂閱資料仍保留，但其 PTT poller 預設關閉；只有分別明確設定 `PTT_COMMENT_JOBS_ENABLED=true` 或 `PTT_PUSHSUM_JOBS_ENABLED=true` 才會額外抓取文章頁或翻頁統計。
- Discord outbox worker 只要 webhook 有效就會啟動，即使 `JOBS_ENABLED=false` 也會清空先前留下的 backlog；沒有有效 webhook 時 backlog 保留在 Redis。
- `RECONCILE_SUBSCRIPTION_INDEXES=true` 會在接受 API 請求前，以 user JSON 為唯一真相精確重建通知 membership；可清理由舊版留下的孤兒索引。訂閱 command 本身也用 Redis `WATCH/MULTI/EXEC`，讓 user 與所有衍生索引一次提交。
- `GOPS_ADDR` 預設空白，因此診斷 port 不會對外開啟。
- `AUTH_USER` 或 `AUTH_PW` 未設定時，驗證 API 回傳 503，而不是接受空白 Basic Auth。

停止服務：

```bash
docker compose down
```

關機時會先停止 PTT 輪詢與 cron，讓 outbox worker 再盡力排空 15 秒（可用 `DISCORD_OUTBOX_SHUTDOWN_DRAIN` 調整）後退出。尚未送完的事件與分段進度留在 Redis，下一次啟動會回收過期 lease 並續送；Compose 的 stop grace period 仍預留 2 分 30 秒。只有明確執行 `docker compose down -v` 才會刪除 Redis volume 與待送通知。

## 開發驗證

專案使用 Go 1.25：

```bash
go test -race ./...
go vet ./...
docker build -t ptt-alertor:local .
```

主要 HTTP API：

- `GET /healthz`, `GET /readyz`
- `GET /boards`
- `GET /boards/:board/articles`（會即時存取 PTT，需 Basic Auth）
- `GET /articles`
- `GET /users`, `GET /users/:account`
- `POST /users`, `PUT /users/:account`
- `POST /users/:account/commands`
- `POST /broadcast`

使用者、command、broadcast 與即時 PTT 文章 API 均需以 `AUTH_USER` / `AUTH_PW` 做 Basic Auth；未設定認證時回傳 503。

## License

Apache License 2.0。原始專案作者與貢獻者資訊保留於 Git history。
