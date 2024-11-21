 select
    DATEADD(MINUTE , -60, max(last_mod_time)) as start_time
 from Utilities..DataEnrichment_Mako
 where content_type = 'article';