# PTT 存取限制查核與因應

查核日期：2026-07-13（Asia/Taipei）。

## 結論

PTT 確實有歷史官方禁 bot 政策與封鎖能力；Ptt-Alertor 的專案提交也記錄過 HTTP 429 與為避免 blocked 而調整間隔。但這些不能證明 2026 年門檻或目前是否使用 Cloudflare，也不代表自動抓取已獲站方授權。

## 可驗證證據

- PTT 官方曾明文公告禁止 bot 收集資料：[PttAntiBot 禁 bot 公告](https://www.ptt.cc/bbs/PttAntiBot/M.1308411811.A.567.html)。
- PTT 官方 PttAntiBot 系統公告曾明確表示，會追查 crawler 的實際來源 IP、拒絕連線，甚至封鎖網段：[PttAntiBot 公告](https://www.ptt.cc/bbs/PttAntiBot/M.1369284435.A.567.html)。
- Ptt-Alertor 2017 年的變更明載，收到 PTT HTTP 429 後停止後續步驟：[commit 45cedd8](https://github.com/Ptt-Alertor/ptt-alertor/commit/45cedd8a84fdcd21c220df2871a81819702b3d18)。
- 2022 年又把輪詢間隔由 200ms 調為 250ms，以避免被 ptt.cc block：[commit 2ee42c6](https://github.com/Ptt-Alertor/ptt-alertor/commit/2ee42c60b00d2dc1869ea813dd17b4051893a496)。
- 2022 年 PR 記錄 AWS 環境曾受 Cloudflare/User-Agent 判定影響：[PR #170](https://github.com/Ptt-Alertor/ptt-alertor/pull/170)。這只能證明歷史情況。
- PTT 的公開原始碼仍包含 ban-IP table 能力：[pttbbs banip.c](https://github.com/ptt/pttbbs/blob/master/common/bbs/banip.c)。這不能證明任何特定 IP 或門檻目前生效。
- PTT 官方舊 pttweb 原始碼確認限制級同意 cookie 名稱為 `over18`：[pttweb cookie.go](https://github.com/ptt/pttweb/blob/master/cookie.go)。

2026-07-13 由本機做少量、低頻檢查時，較早的 Atom 與一般 HTML 請求曾回傳 200，`Server` 為 `Cryophoenix`，沒有 Cloudflare response header；但稍後單次 Atom TLS 連線也曾被對端 reset。無法只憑這次 reset 判定是封鎖、邊緣節點或暫時網路問題，也不能據此推算門檻。`robots.txt` 回 404，Atom/HTML 沒有可依循的公開 crawl-delay；這些結果都不代表站方授權任意抓取。

PTT 使用者條款未提供爬取頻率授權，也提醒非 PTT 或未授權網站重製內容可能涉及權利問題：[PTT 使用者條款](https://www.ptt.cc/index.ua.html)。通知只保留功能所需的標題、連結與命中留言，不鏡像文章全文；但已送到 Discord 的訊息不會在 PTT 刪文後自動刪除。對政策有疑慮時應透過 [PTT 聯絡頁](https://www.ptt.cc/contact.html) 詢問授權。

## 本版的技術策略

所有 `www.ptt.cc` 存取共用單一 limiter 與 in-flight slot。這包含 Atom、HTML crawler 與文章存在檢查；不再另做每分鐘的獨立 health probe。Redis 同時保存 cooldown、精確滑動 24 小時的阻擋事件與小時/每日預算，重啟不會清除保護狀態。記錄一次阻擋、裁切舊事件、計數並 max-extend cooldown 在同一個 Redis Lua 操作完成；持久化失敗時程式會 fail-closed，先修復為 24 小時 durable cooldown 才允許下一次 PTT request。

這個 limiter 是單一 App process 內的保護；目前 Compose 也只啟動一個 App。不要直接水平擴充 App replica，否則每個 replica 都會有自己的 limiter，對 PTT 的合計流量會成倍增加。若未來確實需要多副本，必須先做跨程序共享 rate limit 與單一 polling ownership。

處理原則：

1. 預設 request start interval 為 1 秒、burst 1、最大 in-flight 1；可加慢，不建議調快。
2. Redis 預設每小時最多 900、每 UTC 日最多 10,000 個 request；程式硬上限為 1,800/20,000，達上限時不連線。
3. 429 依 `Retry-After`，缺少 header 時採 30 秒起的全域 exponential backoff + jitter，最高 15 分鐘。
4. 403 不嘗試繞過，直接進入 15 分鐘全域冷卻。
5. `PTT_CIRCUIT_BREAKER_ENABLED=true`（預設）時，精確滑動 24 小時內累計 3 次 403/429/challenge/`ECONNRESET`/header 前 EOF 會開啟 24 小時 circuit breaker。明確設為 `false` 只會停用重複訊號造成的 24 小時升級；每次明確 403/429/challenge 的既有 cooldown、全域 request interval、request budget 與 retry 規則仍保留，block observation 也仍會記錄。對端 reset、EOF 與 unexpected EOF 永遠不在同輪重試；另可明確設 `PTT_TRANSPORT_RESET_COOLDOWN_ENABLED=false`，讓這三種 transport signal 不記為 block、也不套用 15 分鐘 cooldown，下一次嘗試只由既有全域 cadence、producer cycle 與 budget 控制。此開關不影響 403、429 或 HTML challenge。啟用 reset cooldown 時，觀察到 block 後會用獨立 5 秒 bounded context 持久化，不受原 HTTP request 取消影響。500/502/503/504 與其他傳輸錯誤採短退避，404 直接回報。
6. Atom 若是 403/429/5xx，不立刻 fallback HTML。只有其他 Atom/解析問題才使用 HTML；結構完整的 HTML 成功後將該板持久快取為 HTML-only 6 小時，Atom 與 HTML 都失敗則為該板持久退避 15 分鐘，避免下一輪再次產生雙重流量。Backoff 只靠 TTL 到期，並行成功請求不能刪除另一請求剛寫入的較新 backoff。
7. 每個 PTT response body 都必須關閉，slot 才會釋放；整合測試涵蓋序列化與 backlog 間隔。
8. 使用誠實且可設定的 `PTT_USER_AGENT`，禁止偽裝 Googlebot、代理輪換、CAPTCHA bypass 等規避手段。
9. 限制級頁面只有在操作者明確設定 `PTT_OVER18=true` 後才加入 cookie；否則 crawler 回傳明確的 consent error。
10. Atom 小視窗內找不到已保存的文章邊界、且所有項目都比 cursor 新時，才依 `PTT_CATCHUP_MAX_PAGES` 從 HTML 新頁向舊頁有限回溯。完整 `M/G.<timestamp>.A.<hash>` identity 保留同秒多篇；抓取失敗或達上限時不更新 cursor。
11. 看板從無有效訂閱轉為啟用時，訂閱 transaction 會原子寫入啟用時間 watermark；第一次 poll 只通知啟用時間之後的文章，不以靜默 baseline 吞掉等待 limiter 期間的新文。若這段期間超出 Atom 視窗，仍走相同的 bounded catch-up。
12. 推爆統計依 `PTT_PUSHSUM_MAX_PAGES` 限制每輪 HTML 頁數；只有完整到達 48 小時邊界或第 1 頁才提交通知狀態。中途錯誤、無法解析或達頁數上限都保留舊狀態重試。
13. `BOARD_HIGH` 只是優先順序提示；每輪會與實際有訂閱者的看板取交集，不為未訂閱看板增加流量。
14. 留言 cursor 保存完整 occurrence snapshot、隨機 tracking epoch 與 hash-chain revision，並在 enqueue 前先保存不可變 pending transition。Stage、無事件 advance 與 enqueue 後 commit 都以完整 Redis state JSON 做 CAS；只有 command 建立新訂閱可以無條件 reset lifecycle，因此並行 checker 不能覆寫新 epoch 或刪除別人的 pending。頁面結構不完整、推文欄位/時間無法解析，或已追蹤非空頁突然變空時都不推進 cursor；保守策略可能在極端刪留言情況少量重複，但不會靜默漏掉確定的新 suffix。
15. PTT HTML 最多接受 8 MiB，實際會多讀 1 byte 判斷超限；超限頁不交給 HTML parser，避免被自動補齊的截斷文件推進 cursor。Board HTML 還必須有唯一 main/action/list/paging container，每筆未刪文章需有唯一 canonical link，每筆都需有完整 title/date/author；異常 200 會回 typed error，不會 panic 或提交部分清單。

若要刻意用每 10 分鐘一次的節奏觀察 transport reset，而不是讓模糊的 reset/EOF 訊號啟動額外 cooldown，需同時明確設定：

```dotenv
PTT_REQUEST_INTERVAL=10m
PTT_CIRCUIT_BREAKER_ENABLED=false
PTT_TRANSPORT_RESET_COOLDOWN_ENABLED=false
```

這個組合仍不會在同輪重試 reset/EOF，且不會略過明確 403、429、HTML challenge 的既有 cooldown，也不會略過每小時／每日 request budget。

若仍持續看到 403/429，應先停止 jobs、延長 interval/cooldown 並檢查 log；不要用更多 IP 或併發去試探未知門檻。
cooldown、circuit breaker 計數與 request budget 都存在 Redis；不要刪除這些 key 或重建 volume 來清除保護。持續被拒絕時應設 `JOBS_ENABLED=false`、聯絡 PTT，不應換 IP 或冒充 UA。
