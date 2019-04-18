--
-- Created by IntelliJ IDEA.
-- User: Wind
-- Date: 04/17/2019
-- Time: 17:50
-- To change this template use File | Settings | File Templates.
--
local lDes = KEYS[1]
local startTime = tonumber(ARGV[1])
local sKey = lDes .. ':start'
local exist = tonumber(redis.call('get', sKey))
if exist ~= nil then
    if exist < startTime then
        redis.call('rename', lDes, lDes .. ':last')
        redis.call('rename', sKey, lDes .. ':plast')
        redis.call('set', sKey, startTime)
    else return false
    end
else
    redis.call('set', sKey, startTime)
end
local msg = ARGV[2]
redis.call('zincrby', lDes, 1, msg)
local mDes = KEYS[2]
redis.call('lpush', mDes, msg)
redis.call('ltrim', mDes, 0, 99)
return true

