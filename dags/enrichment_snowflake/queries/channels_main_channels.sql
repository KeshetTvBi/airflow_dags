SELECT
    site,
    cmc.Id,
    ChildChannelId as channel_id,
    cmc.ChannelName as channel_name,
    Lvl as Depth,
    tree.ChannelId as main_channel_id,
    tree.ChannelName as main_channel_name,
    cmc.ChannelPath as channel_path,
    Lvl - 1 as channel_depth,
    vgn.lastModTime
FROM [vcmcon].[vcmcon].EXT_TB_ChannelTree tree with(nolock)
    -- מביא רק את הערוצים הראשייים שבהם משנים את הערוץ הראשי לרמה מתחת
INNER JOIN (select Id, ChannelId,ChannelName,ChannelPath,EventsDepth
            ,CASE MainChannelId
              WHEN '31750a2610f26110VgnVCM1000005201000aRCRD' THEN 'n12'
              WHEN 'b5f4c13070733210VgnVCM2000002a0c10acRCRD' THEN '12plus'
              WHEN 'cf25c425b37bc710VgnVCM100000700a10acRCRD' THEN 'tech12'
              ELSE 'mako'
              END as site
            from [vcmcon].[vcmcon].EXT_TB_ChannelsMainChannel with(nolock)
            where MainChannelId IN ('cf25c425b37bc710VgnVCM100000700a10acRCRD','b5f4c13070733210VgnVCM2000002a0c10acRCRD','31750a2610f26110VgnVCM1000005201000aRCRD')
        ) cmc ON cmc.ChannelId = tree.ChildChannelId and cmc.EventsDepth = tree.Lvl
INNER JOIN [vcmsys].[vcmsys].vgnCMSObject vgn with(nolock) ON cmc.ChannelId = vgn.id

UNION ALL -- החלק השני, מחזיר רק את הערוצים הראשיים של חדשות,טק12ו 12פלוס
select
    CASE MainChannelId
          WHEN '31750a2610f26110VgnVCM1000005201000aRCRD' THEN 'n12'
          WHEN 'b5f4c13070733210VgnVCM2000002a0c10acRCRD' THEN '12plus'
          WHEN 'cf25c425b37bc710VgnVCM100000700a10acRCRD' THEN 'tech12'
          ELSE 'mako'
          END as site,
    Id,
       ChannelId,
       ChannelName,
       Depth,
       MainChannelId,
       MainChannelName,
       ChannelPath,
       EventsDepth,
       vgn.lastModTime
FROM [vcmcon].[vcmcon].EXT_TB_ChannelsMainChannel cmc
INNER JOIN [vcmsys].[vcmsys].vgnCMSObject vgn with(nolock)
    ON cmc.ChannelId = vgn.id
WHERE cmc.ChannelId IN ('cf25c425b37bc710VgnVCM100000700a10acRCRD','b5f4c13070733210VgnVCM2000002a0c10acRCRD','31750a2610f26110VgnVCM1000005201000aRCRD')
UNION ALL -- החלק השלישי, מחזיר את הערוצים שאינם של חדשות,טק12ו 12פלוס, כי במאקו הם כבר בתצורה הזו
select 'mako' as site,
       cmc.Id,
       ChannelId,
       ChannelName,
       Depth,
       MainChannelId,
       MainChannelName,
       ChannelPath,
       EventsDepth,
       vgn.lastModTime
FROM [vcmcon].[vcmcon].EXT_TB_ChannelsMainChannel cmc
INNER JOIN [vcmsys].[vcmsys].vgnCMSObject vgn with(nolock)
    ON cmc.ChannelId = vgn.id
WHERE cmc.MainChannelId not IN ('cf25c425b37bc710VgnVCM100000700a10acRCRD','b5f4c13070733210VgnVCM2000002a0c10acRCRD','31750a2610f26110VgnVCM1000005201000aRCRD');