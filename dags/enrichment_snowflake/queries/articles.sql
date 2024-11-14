--declare @start_time as datetime = '2024-11-12 15:00'
--select top 10 *
--FROM [vcmcon].[vcmcon].TB_Articles as art WITH(nolock)
--INNER JOIN [vcmsys].[vcmsys].vgnAsMoMap map WITH(nolock) ON art.ID=map.keyString1
--INNER JOIN [vcmsys].[vcmsys].vgnCMSObject md WITH(nolock) ON map.recordId=md.id
--LEFT JOIN [vcmsys].[vcmsys].vgnAsChannelFileAssociation cfa with(nolock) on map.recordId = cfa.vcmObjectId
--LEFT JOIN [vcmsys].[vcmsys].vgnAsMoMap mapChn with(nolock) on cfa.channelId = mapChn.keyString1
-- LEFT JOIN (
--SELECT mm.recordId as ID
--, count(case when objectTypeId = '9af8ddd3d37c5110VgnVCM1000008249a8c0____' then objectTypeId end) as vids
--, count(case when objectTypeId = '0d08eb62fe263110VgnVCM100000ee01000a____' then objectTypeId end) as pics
--FROM [vcmcon].[vcmcon].TB_Articles a with(nolock)
--LEFT join [vcmcon].[vcmcon].[TB_ExtRelatedContent] e with(nolock) on a.ID = e.Object_ID
--INNER join [vcmsys].[vcmsys].vgnAsMoMap mm with(nolock) on a.ID = mm.keyString1
--INNER join [vcmsys].[vcmsys].[vgnAsMoMetaData] md with(nolock) on e.ExtContent_ID = md.contentMgmtId
--AND objectTypeId in ('9af8ddd3d37c5110VgnVCM1000008249a8c0____', '0d08eb62fe263110VgnVCM100000ee01000a____')
--group by mm.recordId
--) AVP on AVP.ID = map.recordId
--WHERE md.lastModTime > @start_time


select top 10 *  FROM [vcmcon].TB_Articles