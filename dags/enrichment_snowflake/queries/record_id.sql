select
    map.keyString1,
    map.recordId,
    md.lastModTime
from [vcmsys].[vcmsys].vgnAsMoMap map with(nolock)
inner join [vcmsys].[vcmsys].vgnCMSObject md with(nolock) ON map.recordId = md.id
where md.lastModTime > '{timestamp}';