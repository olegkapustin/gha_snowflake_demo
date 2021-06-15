-- **************************************************************************************************************************************************
--                                                                                                         /                                       
--             ---      ---------      ------         ---         ---            ---   ---   ---          /    ---               ------   ---------
--  o  o  o        o                 o           o  o     o     o     o  o     o           o        o    /         o  o     o                      
-- **************************************************************************************************************************************************

use GHA.raw;

-- ****************************************************************************
-- *** json schema research ***
-- ****************************************************************************

-- flatten first level:
--   * we use `flatten` table function - note the syntax
--   * we limit to particular event type as schemas are different for
--     various event types
select
  f.*
from
  gha as g,
  table(flatten(g.record)) as f
where
  g.record:"type"='PushEvent'
limit 100;

-- flattern entire hierarchy
-- Note: this will take 8m5s on xs - see optimized version below
select distinct
  f.key,
  f.path
from
  gha as g,
  table(flatten(g.record, recursive => true)) as f
where
  g.record:"type"='PushEvent'
order by path
limit 100;

-- we limit input here and explicitly cast field to `string`
select distinct
  f.key,
  f.path
from
  (
    select record 
    from gha
    where record:"type"::string='PushEvent'
    limit 1000
  ) as g,
  table(flatten(g.record, recursive => true)) as f
where
  g.record:"type"='PushEvent'
order by path
limit 100;

-- each array item manifest into separate path - let's clean that
select distinct
  f.key,
  regexp_replace(f.path, '\\[.+\\]', '[]') as norm_path
from
  (
    select record 
    from gha
    where record:"type"::string='PushEvent'
    limit 1000
  ) as g,
  table(flatten(g.record, recursive => true)) as f
where
  g.record:"type"='PushEvent'
order by norm_path
limit 100;

-- ****************************************************************************
-- *** "My first dimension" ***
-- ****************************************************************************

-- let's look at actors
select
  record:actor
from
  gha
limit 100;

-- first projection - actor_dim
create or replace view actor_dim
as
select distinct
  record:actor:id::number as actor_id,
  record:actor:display_login::string as actor_display_login,
  record:actor:login::string as actor_login,
  record:actor:url as actor_url,
  record:actor as __record,
  md5(record:actor) as __key
from
  gha
--limit 100; -- << uncomment to check the select statement if needed

-- check for missing records
select count(*) from actor_dim where actor_id IS NULL

select * from actor_dim limit 1000;

-- Let's now mark bots
create or replace view actor_dim
as
select distinct
  record:actor:id::number as actor_id,
  record:actor:display_login::string as actor_display_login,
  record:actor:login::string as actor_login,
  record:actor:url as actor_url,
  case 
    when actor_login like '%bot' or actor_login like '%[bot]' then 'bot'
    else 'user'
  end as actor_type,
  record:actor as __record,
  md5(record:actor) as __key
from
  gha

-- 4m10s on xs - it's time to materialize
select
  actor_type,
  count(*) as cnt
from
  actor_dim
group by
  actor_type

create or replace schema gha.silver
create or replace schema gha.gold

-- 4m37s on xs
create or replace transient table silver.actor_dim
as
select * from raw.actor_dim

select * from silver.actor_dim limit 1000;

create or replace transient table gold.actor_dim clone silver.actor_dim

select
  actor_type,
  count(*) as cnt
from
  gold.actor_dim
group by
  actor_type

select * from gold.actor_dim where actor_login like '%put_your_last_name_here%'

-- ****************************************************************************
-- *** More dimensions - org and repo ***
-- ****************************************************************************

-- org_dim
create or replace view raw.org_dim
as
select distinct
  record:org:id::number as org_id,
  record:org:login::string as org_login,
  record:org:url as org_url,
  record:org as __record,
  md5(record:org) as __key
from
  raw.gha
where org_id is not null

-- 1m33s on xs
create or replace transient table silver.org_dim
as
select * from raw.org_dim

create or replace transient table gold.org_dim clone silver.org_dim

-- you'll be surprised with the outcome of this check ;)
select
  org_login,
  count(*) as cnt
from
  gold.org_dim
group by org_login
having count(*)>1
order by cnt desc
limit 100;

-- let's explore this org
select * from gold.org_dim where org_login='put_your_org_here'

-- repo_dim
create or replace view raw.repo_dim
as
select distinct
  record:repo:id::number as repo_id,
  record:repo:name::string as repo_name,
  record:repo:url as repo_url,
  record:repo as __record,
  md5(record:repo) as __key
from
  raw.gha
where repo_id is not null

create or replace transient table silver.repo_dim
as
select * from raw.repo_dim

create or replace transient table gold.repo_dim clone bronze.repo_dim

-- another surprize ;)
select
  repo_name,
  count(*) as cnt
from
  gold.repo_dim
group by repo_name
having count(*)>1
order by cnt desc
limit 100;

-- good news - no surprize here!
select
  __key,
  count(*) as cnt
from
  gold.repo_dim
group by __key
having count(*)>1
order by cnt desc
limit 100;

-- ****************************************************************************
-- *** It's time for hard facts :O ***
-- ****************************************************************************

create or replace view raw.push_event_fact
as
select
  g.record:id::number as event_id,
  g.record:public::string as public,
  g.record:created_at::timestamp as created_at_ts,
  md5(g.record:actor) as __key_actor,
  md5(g.record:org) as __key_org,
  md5(g.record:repo) as __key_repo
from
  raw.gha as g
where
  g.record:"type"='PushEvent'

-- 4m23s on xs - 87M records
create or replace transient table bronze.push_event_fact
as
select * from raw.push_event_fact

create or replace transient table gold.push_event_fact clone bronze.push_event_fact

-- ****************************************************************************
-- *** Let's have some fun at demo fin :-] ***
-- ****************************************************************************

-- show my repos
select distinct
  my_repo.__key as __key_repo,
  my_repo.repo_name
from
  gold.actor_dim as me
  inner join gold.push_event_fact my_pe on me.__key=my_pe.__key_actor
  inner join gold.repo_dim as my_repo on my_pe.__key_repo=my_repo.__key
where
  me.actor_login='put_your_login_here'

-- who works on my repos?
with myrepos as
(
  select distinct
    my_repo.__key as __key_repo,
    my_repo.repo_name
  from
    gold.actor_dim as me
    inner join gold.push_event_fact my_pe on me.__key=my_pe.__key_actor
    inner join gold.repo_dim as my_repo on my_pe.__key_repo=my_repo.__key
  where
    me.actor_login='put_your_login_here'
)
select
  a.actor_login,
  myrepos.repo_name,
  count(*) as cnt
from
  myrepos
  inner join gold.push_event_fact as pe on pe.__key_repo=myrepos.__key_repo
  inner join gold.actor_dim as a on pe.__key_actor=a.__key 
group by 1, 2

-- Who works together?
with repo_actors as
(
  select 
    a.actor_login,
    pe.__key_repo,
    count(*) as cnt
  from
    gold.push_event_fact as pe
    inner join gold.actor_dim as a on pe.__key_actor=a.__key 
  where
    a.actor_type='user' -- we don't wanna bots
  group by 1, 2
)
select
  r.repo_name,
  ra_left.actor_login as left_login,
  ra_left.cnt as left_cnt,
  ra_right.actor_login as right_login,
  ra_right.cnt as right_cnt,
  left_cnt+right_cnt as total_cnt  
from
  repo_actors as ra_left
  inner join repo_actors as ra_right 
    on ra_left.__key_repo=ra_right.__key_repo 
       and ra_left.actor_login>ra_right.actor_login -- using `<>` results in 2 records 
  inner join gold.repo_dim as r on ra_left.__key_repo=r.__key
where ra_left.cnt/ra_right.cnt between 0.2 and 1.8 -- equal contributors
order by total_cnt desc
limit 100;

-- ****************************************************************************
-- *** P.S.: Gentle reminder ***
-- ****************************************************************************

select 'raw.gha' as tbl, count(*) as cnt from raw.gha
union all
select  'gold.push_event_fact' as tbl, count(*) as cnt from gold.push_event_fact

-- !!! 21m9s on xs
select
  min(len(record)) as min_record_len,
  avg(len(record)) as avg_record_len,
  max(len(record)) as max_record_len
from raw.gha

-- result:
/*
{
  "MIN_RECORD_LEN": 364,
  "AVG_RECORD_LEN": 5028.913415,
  "MAX_RECORD_LEN": 8284109
}
*/