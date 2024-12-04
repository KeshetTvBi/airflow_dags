select
    vfl.*,
    vfi.duration
from KNM.dbo.VideoFilesLocation vfl with (nolock)
left join (select
               ID,
               MAX(duration) as duration
           from KNM.dbo.VideoFilesInformation with (nolock)
           group by ID) vfi on vfi.ID = vfl.ID
where vfl.ModifiedDate > '{timestamp}';