select
    cfa.*
from [vcmsys].[vcmsys].vgnAsChannelFileAssociation cfa with(nolock)
inner join [vcmsys].[vcmsys].vgnCMSObject md with(nolock) on md.id = cfa.vcmObjectId
where md.lastModTime > '{timestamp}';