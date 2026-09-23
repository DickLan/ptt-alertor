# VPS-B deployment and cutover runbook

This runbook keeps polling disabled while VPS-B is staged. Do not perform the
snapshot restore, enable `JOBS_ENABLED`, change DNS/proxy routing, or stop the
VPS-A service without an explicit cutover decision.

## Node roles

VPS-B uses this polling boundary:

```dotenv
BOARD_ALLOWLIST=HardwareSale,MacShop,PC_Shopping
BOARD_HIGH=HardwareSale
STOCK_QUANT_NOTIFY_ENABLE=false
```

`BOARD_ALLOWLIST` is enforced both when the `boards` subscription index is
rebuilt and immediately before a board worker fetches. User documents and
subscriber indexes for other boards remain intact, so a migration does not
discard settings. An unset or blank value preserves the historical unrestricted
behavior for VPS-A and existing deployments. When configured, attempts to add
a subscription outside the boundary return HTTP 400 with a generic message;
existing out-of-bound subscriptions can still be removed without deleting
their migrated board cursor. VPS-A's final role is:

```dotenv
JOBS_ENABLED=false
STOCK_QUANT_NOTIFY_ENABLE=true
```

During staging, VPS-B must use `JOBS_ENABLED=false`. Change it to `true` only
at the cutover step after the VPS-A general poller is stopped and the Redis
snapshot is restored and checked.

## Redis key inventory

The application uses these persisted key families. A full RDB snapshot is the
safest migration unit because cursor, outbox, dedupe, subscription, cooldown,
and rate-budget state must share one consistency point.

| Key or pattern | Redis type | Purpose |
| --- | --- | --- |
| `user:<account>` | string | User profile and configured subscriptions |
| `boards` | set | Derived list consumed by the normal board poller |
| `board:<board>` | string | Article high-water cursor/snapshot |
| `keyword:<board>:subs` | set | Keyword subscribers by board |
| `author:<board>:subs` | set | Author subscribers by board |
| `article:<code>:subs` | set | Comment-tracking subscribers |
| `article:<code>:detail` | hash | Cached article detail |
| `commentcursor:<code>` | string | Durable comment cursor |
| `commentcursor:<code>:pending` | string | Staged comment transition |
| `pushsum:boards` | set | Boards with push-sum subscriptions |
| `pushsum:<board>:subs` | set | Push-sum subscribers by board |
| `pushsum:<account>:<board>:<kind>:<revision>:{base,bench,initialized}` | set/string | Push-sum baseline, done state, and revision |
| `ptta:discord:outbox:ready` | sorted set | General Discord events ready to deliver |
| `ptta:discord:outbox:leased` | sorted set | General Discord events leased to a worker |
| `ptta:discord:outbox:item:<event-id>` | hash | Immutable event payload and delivery progress |
| `ptta:discord:outbox:done:<event-id>` | string | Delivered-event dedupe marker, normally 30-day TTL |
| `ptta:discord:outbox:webhook-not-before` | string | Shared Discord backoff deadline |
| `counter:alert` | string | Delivered alert counter |
| `ptt:access:cooldown` | string | Global PTT cooldown; must not be cleared |
| `ptt:access:blocks` | sorted set | Recent Cloudflare/block observations |
| `ptt:access:block-sequence` | string | Monotonic member sequence for block observations |
| `ptt:access:requests:hour:<bucket>` | string | Persistent hourly request counter |
| `ptt:access:requests:day:<bucket>` | string | Persistent daily request counter |
| `ptt:board-fetch:<board>:mode` | string | Temporary HTML-only fetch mode |
| `ptt:board-fetch:<board>:backoff` | string | Per-board persistent fetch backoff |
| `top:{keywords,authors,pushsum}` | sorted set | Generated popularity lists |
| `ptta:stock-quant:{authors,state,cursor,lock,invalid,history,sent_count}` | mixed | Expert Stock watcher state owned by VPS-A after cutover |
| `ptta:stock-quant:outbox:*` | mixed | Independent expert Stock Discord outbox |

`alert-counter` and `ptta:stock-quant:sent_counter` are Pub/Sub channel names,
not persisted keys. In-memory ten-minute dedupe state is also not persisted;
the durable outbox done markers provide restart-safe dedupe.

Before cutover, inventory VPS-A locally without printing values or secrets:

```bash
docker compose exec -T redis redis-cli --scan | sort >redis-key-inventory.txt
docker compose exec -T redis redis-cli DBSIZE
docker compose exec -T redis redis-cli ZCARD ptta:discord:outbox:ready
docker compose exec -T redis redis-cli ZCARD ptta:discord:outbox:leased
docker compose exec -T redis redis-cli --scan --pattern 'user:*' | wc -l
docker compose exec -T redis redis-cli --scan --pattern 'ptta:discord:outbox:done:*' | wc -l
```

Treat `redis-key-inventory.txt` as sensitive because user account identifiers
may appear in key names. Do not attach it to CI logs or commit it.

## Consistent Redis migration

Use a short write-free cutover window. Pausing only the poller is insufficient
if subscription/API requests can still change user records or enqueue events.

1. Back up the current VPS-A Redis volume and record its SHA-256 digest.
2. Stop or temporarily reject general subscription and broadcast writes at the
   VPS-A proxy. Keep the Stock integration route unchanged.
3. Recreate VPS-A with `JOBS_ENABLED=false` and
   `STOCK_QUANT_NOTIFY_ENABLE=true`, then verify no general poller starts.
4. Wait until both general outbox sorted sets are zero. If a leased event does
   not clear, preserve it in the snapshot; never delete it to make the count
   look clean.
5. Stop the VPS-A app briefly so user/index/cursor/outbox state cannot change.
6. Run `BGSAVE`, wait for `LASTSAVE` to advance, then copy `/data/dump.rdb`
   from the Redis container. Record the RDB digest and keep the prior backup.
7. Copy the RDB to VPS-B over an authenticated channel and verify the digest.
8. Import it into a newly created Docker volume. Set
   `REDIS_VOLUME_NAME=<new-volume>` in VPS-B's local `.env`; do not overwrite
   the previous volume. Start Redis only and confirm its log reports a clean
   load.
9. With VPS-B polling still disabled, compare database size, user count,
   selected cursor existence, outbox counts, cooldown/rate-state existence,
   and `TYPE`/`PTTL` for the key families above.
10. Start the VPS-B app with `JOBS_ENABLED=false`. Startup reconciliation will
    retain user documents but restrict the `boards` set to allowed active
    subscriptions. Confirm `SMEMBERS boards` contains no value outside
    `hardwaresale`, `macshop`, and `pc_shopping`. If one of the three is absent,
    verify whether it actually has an enabled Discord keyword/author
    subscription before adding or changing any state.
11. Route general API writes to VPS-B, then set `JOBS_ENABLED=true` on VPS-B
    and recreate only its app container. This is the first permitted general
    polling start.
12. Restart/verify VPS-A in its Stock-only role and restore the Stock integration
    route to VPS-A. The copied `ptta:stock-quant:*` keys on VPS-B remain inert
    because its Stock watcher is disabled.

Do not clear `ptt:access:*` or `ptt:board-fetch:*`, shorten their TTLs, or raise
request limits during migration. Those keys carry the Cloudflare cooldown and
rate safety state across the handoff.

## Cutover checks

Run these on VPS-B after the restore and before enabling polling:

```bash
docker compose exec -T redis redis-cli SMEMBERS boards | sort
docker compose exec -T redis redis-cli EXISTS board:hardwaresale
docker compose exec -T redis redis-cli EXISTS board:macshop
docker compose exec -T redis redis-cli EXISTS board:pc_shopping
docker compose exec -T redis redis-cli ZCARD ptta:discord:outbox:ready
docker compose exec -T redis redis-cli ZCARD ptta:discord:outbox:leased
curl --fail --silent --show-error http://127.0.0.1:9090/healthz
curl --fail --silent --show-error http://127.0.0.1:9090/readyz
```

After enabling polling, inspect logs for the three allowed names, absence of
the Stock expert watcher, HTTP 403/429/challenge signals, and request-budget
exhaustion. Restart the app once and verify no old article is enqueued again.
Restart Redis once and verify the selected named volume still contains the same
board cursors and outbox/done counts.

## API and proxy routing

The cleanest split uses two HTTPS hostnames:

- The general API/UI hostname points to VPS-B and carries `/users`, `/boards`,
  `/keyword`, `/author`, `/pushsum`, `/articles`, `/broadcast`, `/ws`, health,
  redirects, and static pages.
- A Stock integration hostname remains on VPS-A and carries only
  `/integrations/stock-quant/*`. `EXPERT_NOTIFICATION_API_URL` continues to use
  this VPS-A hostname.

If one hostname must be retained, its reverse proxy must route
`/integrations/stock-quant/*` to VPS-A and all general paths to VPS-B. Preserve
HTTP Basic authentication and forward only over an authenticated private link
or HTTPS between hosts. VPS-B's application listener stays on
`127.0.0.1:9090`; the public proxy terminates TLS locally. Perform proxy and DNS
changes only in the explicit cutover window, after testing the proposed routes
with host-header overrides.

## GitHub Actions deployment

Before production cutover, `deploy-vps-b.yml` has only a manual
`workflow_dispatch` trigger. The operator must supply one full 40-character
commit SHA; the workflow verifies the checkout matches that exact SHA and
contains `e05bff8`, tests with Go 1.25, builds one commit-tagged Docker image,
and transfers that exact image over SSH. It never transfers `.env`.

Configure the `vps-b` GitHub environment with:

- `VPS_B_HOST`
- `VPS_B_USER`
- `VPS_B_SSH_KEY`
- `VPS_B_SSH_PORT` when SSH does not use port 22
- `VPS_B_KNOWN_HOSTS`, populated out of band from the verified VPS-B host key

Enable required-reviewer approval on the GitHub `vps-b` environment before the
workflow is used. The remote user needs Docker access and write access to
`/opt/ptt-alertor`. Deployments share one concurrency group. SSH host-key
verification is mandatory. The transferred image archive is checked against a
SHA-256 digest before `docker load`, and temporary archives are removed after
success or failure.

A release becomes current only after both loopback health endpoints pass;
failure restores the previous image and compose file and checks both endpoints
again. On the first workflow run, a healthy manually staged app is registered
as a bootstrap release before it is replaced, so it is also a rollback target.
If no prior app exists, a failed first deployment stops the failed app. Cleanup
is limited to the `ptt-alertor-vps-b:<commit>` repository and retains only the
new and immediately previous deployment images. It never runs
`docker system prune`.

## SSH hardening after deployment validation

Do not change root login, password authentication, or the firewall as part of
this unreviewed staging change. After the deployment path is tested:

1. Create a dedicated deploy user with its own SSH public key.
2. Grant only the Docker and `/opt/ptt-alertor` permissions needed by the
   reviewed deployment script; avoid general-purpose sudo where possible.
3. Run a manual deployment and a separate interactive recovery test as that
   user, including host-key verification and rollback.
4. Confirm an independent administrative key still works.
5. Only then disable SSH password authentication and decide whether direct
   root login should be disabled. Apply firewall changes in a separate reviewed
   maintenance window with an active recovery session.
