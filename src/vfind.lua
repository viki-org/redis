-- @desc: Retrieves a range of elements from a sorted set and filters them based on allow, block, and filter sets.
-- @usage: redis-cli EVAL "$(cat vfind.lua)" <numkeys> <zset_key> [allow_keys...] [blocked_keys...] [filter_keys...] <offset> <count> <upto> <direction> <include_blocked> <allow_count> <block_count> <filter_count>
-- @return: list of results, and count of results

-- quicksort functions as require() is disabled by redis
--[[
 * Three-way partition function for quicksort
 *
 * Partitions an array into three sections:
 *   - Elements smaller than the pivot
 *   - Elements equal to the pivot
 *   - Elements greater than the pivot
 *
 * Parameters:
 *   arr (table) - The list to be partitioned
 *   from (number) - Start index of the partition
 *   to (number) - End index of the partition
 *   pivot_val (any) - Pivot element used for partitioning
 *   less_than (function) - Comparator function that determines ordering
 *
 * Returns:
 *   (number, number) - Indices marking the end of the "smaller" section
 *                      and the beginning of the "larger" section.
]]
local function three_way_partition(arr, from, to, pivot_val, less_than)
    local i = from
    local j = from
    local k = to
    while j <= k do
        if less_than(arr[j], pivot_val) then
            arr[i], arr[j] = arr[j], arr[i]
            i, j = i + 1, j + 1
        elseif less_than(pivot_val, arr[j]) then
            arr[j], arr[k] = arr[k], arr[j]
            k = k - 1
        else
            j = j + 1
        end
    end
    return i - 1, k + 1
end


--[[
 * Quicksort function
 *
 * Sorts a list in place using a recursive three-way quicksort algorithm.
 *
 * Parameters:
 *   list (table) - The table (list) to be sorted
 *   less_than (function) [optional] - Comparator function that determines sorting order
 *   choose_pivot (function) [optional] - Function to choose a pivot index
 *
 * Returns:
 *   None (modifies the input table in place)
]]
local function quicksort(list, less_than, choose_pivot)
    less_than = less_than or function(a, b)
        return a < b
    end
    choose_pivot = choose_pivot or math.random

    local function sort(from, to)
        if from >= to then
            return
        end
        local pivot_val = list[choose_pivot(from, to)]
        local smaller_to, larger_from = three_way_partition(list, from, to, pivot_val, less_than)
        if smaller_to - from < to - larger_from then
            sort(from, smaller_to)
            sort(larger_from, to)
        else
            sort(larger_from, to)
            sort(from, smaller_to)
        end
    end

    sort(1, #list)
end

-- helper functions
local function is_in_all_filters(start_index, item, filter_keys, filter_count)
    for i = start_index, filter_count do
        if redis.call("SISMEMBER", filter_keys[i], item) == 0 then
            return false
        end
    end
    return true
end

local function is_blocked(item, block_keys, block_count)
    for i = 1, block_count do
        if redis.call("SISMEMBER", block_keys[i], item) == 1 then
            return true
        end
    end
    return false
end

local function is_allowed(item, allow_keys, allow_count)
    if allow_count == 0 then return true end
    for i = 1, allow_count do
        if redis.call("SISMEMBER", allow_keys[i], item) == 1 then
            return true
        end
    end
    return false
end

local function compare_sets_by_cardinality(a, b)
    return redis.call("SCARD", a) < redis.call("SCARD", b)
end

local function vfindBySmallestFilter(zset_key, filter_keys, filter_count, allow_keys, allow_count, block_keys,
                                     block_count, offset, count, upto, direction, include_blocked)
    local results = {}
    local found, added = 0, 0
    local zset = redis.call(direction == "desc" and "ZREVRANGE" or "ZRANGE", zset_key, offset, -1)

    for _, item in ipairs(zset) do
        -- check if element exists in the zset
        if redis.call("ZSCORE", zset_key, item) == -1 then
            goto continue
        end

        if filter_count > 0 and not is_in_all_filters(2, item, filter_keys, filter_count) then
            goto continue
        end

        local is_item_blocked = is_blocked(item, block_keys, block_count)
        if is_item_blocked and not include_blocked then
            goto continue
        end

        if allow_count > 0 and not is_allowed(item, allow_keys, allow_count) then
            goto continue
        end

        -- add found items
        found = found + 1
        if found > offset and added < count then
            table.insert(results, { id = item, blocked = is_item_blocked })
            added = added + 1
        end

        if added == count and found >= upto then
            break
        end

        ::continue::
    end

    table.insert(results, added)
    return results
end

local function vfindByZSet(zset_key, filter_keys, filter_count, allow_keys, allow_count, block_keys, block_count, offset,
                           count, upto, direction, include_blocked)
    local results = {}
    local found, added = 0, 0
    local zset = redis.call(direction == "desc" and "ZREVRANGE" or "ZRANGE", zset_key, offset, -1)

    for i = 1, #zset do
        local item = zset[i]

        if filter_count > 0 and not is_in_all_filters(1, item, filter_keys, filter_count) then
            goto continue
        end

        local is_item_blocked = is_blocked(item, block_keys, block_count)
        if is_item_blocked and not include_blocked then
            goto continue
        end

        if allow_count > 0 and not is_allowed(item, allow_keys, allow_count) then
            goto continue
        end

        -- add found items
        found = found + 1
        if found > offset and added < count then
            table.insert(results, { id = item, blocked = is_item_blocked })
            added = added + 1
        end

        if added == count and found >= upto then
            break
        end

        ::continue::
    end

    table.insert(results, added)
    return results
end

-- main function
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
    filter_keys[i] = KEYS[allow_count + block_count + i + 1]
end

if filter_count > 0 and filter_keys[1] then
    quicksort(filter_keys, compare_sets_by_cardinality)
    local size = redis.call("SCARD", filter_keys[1])
    local ratio = redis.call("SCARD", zset_key) / size

    if (size < 100 and ratio > 1) or (size < 500 and ratio > 2) or (size < 2000 and ratio > 3) then
        return vfindBySmallestFilter(zset_key, filter_keys, filter_count, allow_keys, allow_count, block_keys,
            block_count, offset, count, upto, direction, include_blocked)
    end
end

return vfindByZSet(zset_key, filter_keys, filter_count, allow_keys, allow_count, block_keys, block_count, offset, count,
    upto, direction, include_blocked)
