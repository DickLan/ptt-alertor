package outbox

import "context"

// Cancel is an optional extension: cancellation is not a Discord delivery and
// must never create a fake message ID or increment delivery counters.
func (store *Redis) Cancel(ctx context.Context, eventID, token string) error {
	if err := validateTransitionIdentity(eventID, token); err != nil {
		return err
	}
	_, err := store.eval(ctx, cancelScript, []string{store.readyKey(), store.leasedKey(), store.itemKey(eventID), store.doneKey(eventID)}, eventID, token, store.doneTTL.Milliseconds())
	return err
}

const cancelScript = luaHelpers + `
require_type(KEYS[1], 'zset')
require_type(KEYS[2], 'zset')
require_type(KEYS[3], 'hash')
require_type(KEYS[4], 'hash')
local exists = redis.call('EXISTS', KEYS[3]) == 1
local done = redis.call('EXISTS', KEYS[4]) == 1
if not exists then
 if done then return 0 end
 error('OUTBOX_NOT_FOUND')
end
if done then error('OUTBOX_CORRUPT') end
if redis.call('HGET',KEYS[3],'lease_token') ~= ARGV[2] then error('OUTBOX_STALE_LEASE') end
if redis.call('HGET',KEYS[3],'state') ~= 'leased' or not redis.call('ZSCORE',KEYS[2],ARGV[1]) then error('OUTBOX_INVALID_STATE') end
local now = now_ms()
local until_ms = tonumber(redis.call('HGET',KEYS[3],'lease_until_ms'))
if not until_ms then error('OUTBOX_CORRUPT') end
if until_ms <= now then error('OUTBOX_LEASE_EXPIRED') end
local hash = redis.call('HGET',KEYS[3],'payload_hash')
local kind = redis.call('HGET',KEYS[3],'kind')
local count = tonumber(redis.call('HGET',KEYS[3],'chunk_count'))
local next_chunk = tonumber(redis.call('HGET',KEYS[3],'next_chunk'))
if not hash or not kind or not count or not next_chunk or next_chunk < 0 or next_chunk >= count then error('OUTBOX_CORRUPT') end
for i=0,next_chunk-1 do
 if not redis.call('HGET',KEYS[3],'message_id:'..i) then error('OUTBOX_CORRUPT') end
end
redis.call('HSET',KEYS[4],'schema','1','payload_hash',hash,'kind',kind,'chunk_count',count,'completed_at_ms',now,'state','cancelled','reason','subscription_removed')
for i=0,next_chunk-1 do redis.call('HSET',KEYS[4],'message_id:'..i,redis.call('HGET',KEYS[3],'message_id:'..i)) end
if tonumber(ARGV[3]) > 0 then redis.call('PEXPIRE',KEYS[4],ARGV[3]) end
redis.call('ZREM',KEYS[1],ARGV[1])
redis.call('ZREM',KEYS[2],ARGV[1])
redis.call('DEL',KEYS[3])
return 1
`
