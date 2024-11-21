select
    furl.*
from vcmcon.[dbo].[FURL_ByItemAndChannel] furl with(nolock)
inner join [vcmsys].[vcmsys].vgnCMSObject md with(nolock) on furl.item_id=md.id
where system_content_type in ('articleins', 'matconinsert', 'VOD','videoins') and md.lastModTime > '{timestamp}';