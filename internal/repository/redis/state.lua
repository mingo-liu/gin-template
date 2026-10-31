local max = 9007199254740991
local function integer(v)
 if not v or not string.match(v, '^%d+$') or (#v > 1 and string.sub(v,1,1)=='0') then return nil end
 local n=tonumber(v)
 if not n or n>max then return nil end
 return n
end
local expected={'set','string','hash','stream','hash'}
for i=1,5 do
 local t=redis.call('TYPE',KEYS[i]).ok
 if t~=expected[i] and not (t=='none' and (i==1 or i==3)) then return redis.error_reply('UNAVAILABLE key type') end
end
if redis.call('HGET',KEYS[5],'state')~='READY' or redis.call('HGET',KEYS[5],'run_id')~=ARGV[5] then return redis.error_reply('UNAVAILABLE recovering or restarted') end
local generation=redis.call('HGET',KEYS[5],'generation')
if not generation or generation=='' then return redis.error_reply('UNAVAILABLE generation') end
local countText=redis.call('GET',KEYS[2])
local count=integer(countText)
local liked=redis.call('SISMEMBER',KEYS[1],ARGV[1])
local versionText=redis.call('HGET',KEYS[3],ARGV[1]) or '0'
local version=integer(versionText)
if not count or not version or count~=redis.call('SCARD',KEYS[1]) or (liked==1 and version==0) then return redis.error_reply('UNAVAILABLE corrupt state') end
local action=ARGV[4]
if action=='QUERY' then return {liked,countText,versionText,0,'OK'} end
if action~='LIKE' and action~='UNLIKE' then return redis.error_reply('invalid action') end
if (action=='LIKE' and liked==1) or (action=='UNLIKE' and liked==0) then
 local code='NOT_LIKED';if liked==1 then code='ALREADY_LIKED' end
 return {liked,countText,versionText,0,code}
end
if redis.call('XLEN',KEYS[4])>=tonumber(ARGV[6]) then return redis.error_reply('UNAVAILABLE backlog limit') end
if version==max or (action=='LIKE' and count==max) or (action=='UNLIKE' and count==0) then return redis.error_reply('UNAVAILABLE overflow') end
local newLiked=0;local newCount=count-1
if action=='LIKE' then newLiked=1;newCount=count+1 end
local newVersion=string.format('%.0f',version+1)
local newCountText=string.format('%.0f',newCount)
redis.call('HSET',KEYS[5],'state','RECOVERING')
redis.call('XADD',KEYS[4],'*','user_id',ARGV[1],'target_type',ARGV[2],'target_id',ARGV[3],'action',action,'version',newVersion,'generation',generation)
if newLiked==1 then redis.call('SADD',KEYS[1],ARGV[1]) else redis.call('SREM',KEYS[1],ARGV[1]) end
redis.call('SET',KEYS[2],newCountText)
redis.call('HSET',KEYS[3],ARGV[1],newVersion)
redis.call('HSET',KEYS[5],'state','READY')
return {newLiked,newCountText,newVersion,1,'OK'}
