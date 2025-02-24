-- Retrieves a range of elements from a sorted set and filters them based on allow, block, and filter sets.
-- Syntax: redis-cli EVAL "$(cat vfind.lua)" <numkeys> <zset_key> [allow_keys...] [blocked_keys...] [filter_keys...] <offset> <count> <upto> <direction> <include_blocked> <allow_count> <block_count> <filter_count>

local zset_key = KEYS[1]

local offset = tonumber(ARGV[1])
local count = tonumber(ARGV[2])
local upto = tonumber(ARGV[3])
local direction = ARGV[4]
local include_blocked = (ARGV[5] == "withblocked")
local allow_count = tonumber(ARGV[6])
local block_count = tonumber(ARGV[7])
local filter_count = tonumber(ARGV[8])

local allow_keys = {}
for i = 1, allow_count do
    allow_keys[i] = KEYS[i + 1]
end

local block_keys = {}
for i = 1, block_count do
    block_keys[i] = KEYS[allow_count + i + 1]
end

local filter_keys = {}
for i = 1, filter_count do
    filter_keys[i] = KEYS[allow_count + block_count + i + 1 ]
end

-- helper functions
local function is_in_all_filters(item)
    for i = 1, filter_count do
        if redis.call("SISMEMBER", filter_keys[i], item) == 0 then
            return false
        end
    end
    return true
end

local function is_blocked(item)
    for i = 1, block_count do
        if redis.call("SISMEMBER", block_keys[i], item) == 1 then
            return true
        end
    end
    return false
end

local function is_allowed(item)
    for i = 1, allow_count do
        if redis.call("SISMEMBER", allow_keys[i], item) == 1 then
            return true
        end
    end
    return false
end

local results = {}
found = 0
added = 0

local zset = redis.call("ZRANGE", zset_key, offset, -1)
if direction == "desc" then
    zset = redis.call("ZREVRANGE", zset_key, offset, -1)
end


for i = 1, #zset do
    local item = zset[i]

    if filter_count > 0 and not is_in_all_filters(item) then
        goto continue
    end

    if is_blocked(item) and not include_blocked then
        goto continue
    end

    if allow_count > 0 and not is_allowed(item) then
        goto continue
    end

    -- add found items
    found = found + 1
    if found > offset and added < count then
        local result = {}
        result["id"] = item
        result["blocked"] = is_blocked(item)
        table.insert(results, result)
        added = added + 1
    end

    if added == count and found >= upto then
        break
    end

    ::continue::
end

table.insert(results, added)

return results