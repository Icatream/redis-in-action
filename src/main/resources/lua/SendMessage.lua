--
-- Created by IntelliJ IDEA.
-- User: Wind
-- Date: 05/10/19
-- Time: 10:53
-- To change this template use File | Settings | File Templates.
--
local countId = KEYS[1]
local zKey = KEYS[2]
local item = ARGV[1]
local mid = redis.call('incr', countId)
redis.call('zadd', zKey, mid, item)
return mid;

