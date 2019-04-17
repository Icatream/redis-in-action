local inventory = KEYS[1]
local itemId = ARGV[1]
local exist = redis.call('sismember', inventory, itemId)
if exist == 0 then return false end
local market = KEYS[2]
local item = KEYS[3]
local price = ARGV[2]
redis.call('zadd', market, price, item)
redis.call('srem', inventory, itemId)
return true