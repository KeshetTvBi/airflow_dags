select
    'vodtype' as fieldType,
    c.value('(value[1])', 'NVARCHAR(100)')  as xvalue,
    c.value('(label[1])', 'NVARCHAR(100)') as xlabel
from (select
          cast(widgetData as XML) as widgetData
      from vcmsys.vcmsys.vgnAsAttrDef with(nolock)
      where name = 'TB-VODType') as ad
cross apply ad.widgetData.nodes('selectOptions/selectOption') AS m(c)
union
select
    'vodnature' as fieldType,
    c.value('(value[1])', 'NVARCHAR(100)') as xvalue,
    c.value('(label[1])', 'NVARCHAR(100)') as xlabel
from (select
          cast(widgetData as XML) as widgetData
      from vcmsys.vcmsys.vgnAsAttrDef with(nolock)
      where name = 'TB-VODNature') as ad
cross apply ad.widgetData.nodes('selectOptions/selectOption') as m(c);