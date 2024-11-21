select
     vr.itemID AS item_id,
     vi.itemID  as main_channel_id,
     count(case when optionTitle like N'%לא%' then [optionTitle] end) as negative,
     count(case when optionTitle not like N'%לא%' then [optionTitle] end) as positive
from (select itemID, optionID
      from [MAKO.KESHET.PRD].[Votes].[dbo].[VoteResults] with(nolock)
      where itemID in
        (select distinct itemID from [MAKO.KESHET.PRD].[Votes].[dbo].[VoteResults] vr with(nolock) where createdDate > '{timestamp}')
     )vr
inner join  [MAKO.KESHET.PRD].[Votes].[dbo].[VoteOptions] vo with(nolock) on vr.optionID = vo.optionID
inner join  [MAKO.KESHET.PRD].[Votes].dbo.VoteItems vi with(nolock) on vo.voteID = vi.voteID
where vo.active = 1
group by vr.itemID, vi.itemID;