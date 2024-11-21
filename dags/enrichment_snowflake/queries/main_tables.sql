select
    tbl.*,
    map.recordId
from [vcmcon].[vcmcon].{table_name} as tbl with(nolock)
inner join [vcmsys].[vcmsys].vgnAsMoMap map with(nolock) on tbl.ID=map.keyString1
inner join [vcmsys].[vcmsys].vgnCMSObject md with(nolock) on map.recordId=md.id
where md.lastModTime > '{timestamp}';
