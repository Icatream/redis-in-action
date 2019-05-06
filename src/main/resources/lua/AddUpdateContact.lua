--
-- Created by IntelliJ IDEA.
-- User: Wind
-- Date: 05/05/19
-- Time: 12:14
-- To change this template use File | Settings | File Templates.
--
local lKey = KEYS[1]
local value = ARGV[1]
redis.call('lrem', lKey, 1, value)
redis.call('lpush', lKey, value)
redis.call('ltrim', lKey, 0, 99)
return true
