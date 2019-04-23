--
-- Created by IntelliJ IDEA.
-- User: Wind
-- Date: 04/22/2019
-- Time: 15:56
-- To change this template use File | Settings | File Templates.
--
local precision = { 60, 300, 3600, 18000, 86400 }
local zsetKey = KEYS[1]
local hashKey = KEYS[2]
local now = tonumber(ARGV[1])
local name = ARGV[2]
local count = tonumber(ARGV[3])
for _, prec in ipairs(precision)
do
    local pnow = math.floor(now / prec) * prec
    local hash = prec .. ':' .. name
    redis.call('zadd', zsetKey, 0, hash)
    redis.call('hincrby', hashKey .. hash, pnow, count)
end
return true