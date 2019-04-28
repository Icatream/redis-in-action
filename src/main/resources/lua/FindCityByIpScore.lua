--
-- Created by IntelliJ IDEA.
-- User: Wind
-- Date: 04/28/2019
-- Time: 10:29
-- To change this template use File | Settings | File Templates.
--
local key = KEYS[1]
local score = tonumber(ARGV[1])
local cityId = redis.call('zrevrangebyscore', key, score, 0, 'limit', 0, 1)
if cityId ~= nil then
end
