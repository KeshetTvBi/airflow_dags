select
    tbl.ID,
    tbl.Object_ID,
    tbl.ExtContent_ID
from [vcmcon].[vcmcon].TB_ExtRelatedContent as tbl with(nolock)
inner join [vcmsys].[vcmsys].vgnAsMoMap map with(nolock) on tbl.Object_ID=map.keyString1
inner join [vcmsys].[vcmsys].vgnCMSObject md with(nolock) on map.recordId=md.id
where md.lastModTime > '{timestamp}';