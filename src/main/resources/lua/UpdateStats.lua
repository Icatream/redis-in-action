--
-- Created by IntelliJ IDEA.
-- User: Wind
-- Date: 04/23/2019
-- Time: 16:23
-- To change this template use File | Settings | File Templates.
--
local des = KEYS[1]
local startHour = tonumber(ARGV[1])
local sKey = des .. ':start'
local exist = tonumber(redis.call('get', sKey))
if (exist ~= nil and exist < startHour) then
    redis.call('rename', des, des .. ':last')
    redis.call('rename', sKey, des .. ':plast')
    redis.call('set', sKey, startHour)
end
local value = tonumber(ARGV[2])
local tmpK1 = 'tmp:k1'
local tmpK2 = 'tmp:k2'
redis.call('zadd', tmpK1, value, 'min')
redis.call('zadd', tmpK2, value, 'max')
redis.call('zunionstore', des, 2, des, tmpK1, 'aggregate', 'min')
redis.call('zunionstore', des, 2, des, tmpK2, 'aggregate', 'max')
redis.call('del', tmpK1, tmpK2)
redis.call('zincrby', des, 1, 'count')
redis.call('zincrby', des, value, 'sum')
redis.call('zincrby', des, value ^ 2, 'sumsq')
return true