select
    f.forum_name AS forumName,
    count(*) as numberOfComments
from [KNM].[dbo].[jforum_forums] f with(nolock)
inner join [KNM].[dbo].[jforum_posts] p with(nolock)  on f.forum_id = p.forum_id
where forum_name like '%RCRD' and p.[need_moderate] = 0
	and p.forum_id in (select forum_id from [KNM].[dbo].[jforum_posts] with(nolock)  where post_time > '{timestamp}' and [need_moderate] = 0)
group by f.forum_name