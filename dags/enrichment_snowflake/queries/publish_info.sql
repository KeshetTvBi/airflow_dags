select
    mm.recordId,
    ot.[name] as contenType,
   convert(datetime, cms.lastModTime at time zone 'Israel Standard Time' at time zone 'UTC') as lastModTime,
   case when md.publishDate=0 then null else dateadd(s, md.publishDate / 1000, '19700101 00:00:00') end as publishDate,
   case when md.unpublishDate=0  then null else dateadd(s, md.unpublishDate / 1000, '19700101 00:00:00') end as unpublishDate,
   case when md.lastPublishDate=0  then null else dateadd(s, md.lastPublishDate / 1000, '19700101 00:00:00') end as lastPublishDate,
   case when md.lastUnpublishDate=0  then null else dateadd(s, md.lastUnpublishDate / 1000, '19700101 00:00:00') end as lastUnpublishDate,
   md.name,
   md.logicalPath,
   md.modCount
from vcmsys.vcmsys.vgnAsMoMap mm with (nolock)
inner join vcmsys.vcmsys.vgnAsMoMetaData md with (nolock) on mm.recordId = md.contentMgmtId
inner join [vcmsys].[vcmsys].[vgnCMSObject] cms with (nolock) on mm.recordId = cms.id
inner join [vcmsys].[vcmsys].vgnAsObjectType ot with (nolock) on md.objectTypeId = ot.id
       and ot.[name] in ( 'articleins', 'videoins', 'matconinsert', 'VOD' )
where (dateadd(s, md.lastPublishDate / 1000, '19700101 00:00:00') >= getdate()-3 or
        dateadd(s, md.lastUnpublishDate / 1000, '19700101 00:00:00') >= getdate()-3);