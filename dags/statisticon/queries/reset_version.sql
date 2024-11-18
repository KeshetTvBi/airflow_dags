with reset_row as (
    select
        *,
        row_number() over (partition by URL order by FirstClickTimestamp DESC) as rn
    from dbo.UrlClickCounters
    )

select ID,
       URL,
       Channel,
       FirstClickTimestamp,
       LastClickTimestamp,
       MaxMinuteAvg,
       MaxAvgTimestamp,
       ClickCounter,
       PortletId,
       URLHashCode,
       TotalCountWeb,
       TotalCountMobile,
       TotalCountMobileApp,
       TotalCountWinApp
from reset_row
where rn = 1;