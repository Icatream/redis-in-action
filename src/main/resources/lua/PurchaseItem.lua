local market = KEYS[1]
local mItem = ARGV[1]
local price = redis.call('zscore', market, mItem)
local lPrice = ARGV[2]
if price ~= lPrice then return false end
local buyer = KEYS[2]
local fKey = KEYS[5]
local funds = tonumber(redis.call('hget', buyer, fKey))
local p = tonumber(price)
if (funds ~= nil and funds > p) then
    local seller = KEYS[3]
    local bInv = KEYS[4]
    local itemId = ARGV[3]
    redis.call('hincrby', seller, fKey, p)
    redis.call('hincrby', buyer, fKey, -p)
    redis.call('sadd', bInv, itemId)
    redis.call('zrem', market, mItem)
    return true
end
return false