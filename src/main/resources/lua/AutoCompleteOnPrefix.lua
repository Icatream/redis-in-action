--
-- Created by IntelliJ IDEA.
-- User: Wind
-- Date: 05/05/19
-- Time: 17:42
-- To change this template use File | Settings | File Templates.
--
local zsKey = KEYS[1]
local limit = tonumber(KEYS[2])
local s = ARGV[1]
local e = ARGV[2]
redis.call('zadd', zsKey, 1, s, 1, e)
local sIndex = redis.call('zrank', zsKey, s)
local eIndex = redis.call('zrank', zsKey, e)
local eRange = math.min(sIndex - 1 + limit, eIndex - 2)
redis.call('zrem', zsKey, s, e)
return redis.call('zrange', zsKey, sIndex, eRange)
