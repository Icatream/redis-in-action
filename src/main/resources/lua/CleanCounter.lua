--
-- Created by IntelliJ IDEA.
-- User: Wind
-- Date: 04/23/2019
-- Time: 11:01
-- To change this template use File | Settings | File Templates.
--
local zKey = KEYS[1]
local hKeyPrefix = KEYS[2]
local passes = tonumber(ARGV[1])
local now = tonumber(ARGV[2])
local sampleCount = tonumber(ARGV[3])
local index = 0
while (index < redis.call('zcard', zKey))
do
    local hash = redis.call('zrange', zKey, index, index)[1]
    local prec = tonumber(string.sub(hash, 0, string.find(hash, ':') - 1))
    local bprec = math.floor(prec / 60)
    if bprec == 0 then bprec = 1 end
    if (passes % bprec) ~= 0 then
        local hKey = hKeyPrefix .. hash
        local cutoff = now - prec * sampleCount
        local hArr = redis.call('hkeys', hKey)
        local clear = true
        for _, k in ipairs(hArr) do
            local i = tonumber(k)
            if i <= cutoff then
                redis.call('hdel', hKey, k)
            else
                clear = false
            end
        end
        if clear then
            redis.call('zrem', zKey, hash)
            index = index - 1
        end
    end
    index = index + 1
end
return true