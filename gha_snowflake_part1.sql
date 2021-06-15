-- **************************************************************************************************************************************************
--                                                                                                         /                                       
--             ---      ---------      ------         ---         ---            ---   ---   ---          /    ---               ------   ---------
--  o  o  o        o                 o           o  o     o     o     o  o     o           o        o    /         o  o     o                      
-- **************************************************************************************************************************************************


-- ****************************************************************************
-- *** setup database and schema ***
-- ****************************************************************************
create database GHA;

create schema GHA.raw;

use GHA.raw;

-- ****************************************************************************
-- *** setup connection to lake ***
-- ****************************************************************************

create or replace stage lake
url = 'azure://<YOUR ACCOUNT NAME>.blob.core.windows.net/lake/'
credentials = (azure_sas_token = '<YOUR SAS TOKEN>');

-- Test we're in
list @lake;
-- this is how we can re-use output from list
SELECT * FROM table(result_scan(last_query_id())) limit 1;

-- ****************************************************************************
-- *** configure format for JSON/GZ ***
-- ****************************************************************************

create or replace file format json_gz_format
type = 'json'
compression = 'gzip'

-- ****************************************************************************
-- *** query file in external stage ***
-- ****************************************************************************
-- by default snowflake tries to parse file as csv
-- extensions are used to hint the compression
-- JSONs use comma as field delimiter, so the query below will output some 
-- result, though it is not what we really need...
select 
  $1,
  $2,
  $3
from @lake/gha/2021-04-01-0.json.gz as j
limit 10;

-- json and parquet are always parsed on a per-record basis first, therefore:
--   - <alias>.$1 - returns entire record (variant)
--   - <alias>.$1:<property> - returns record property (1st level)
--   - <alias>.$1:<property>:<sub_property> - returns nested record property
--   - <alias>.$1:<property>::<type> - casts property to the desired type
--     (note _double_ colon)
select 
  j.$1
from @lake/gha/2021-04-01-0.json.gz (file_format => 'json_gz_format') as j
limit 10;

-- We can also query some file metadata. Namely - original file name and row
-- number.
select 
  metadata$filename as __filename,
  metadata$file_row_number as __row_number,
  j.$1 as record
from @lake/gha/2021-04-01-0.json.gz (file_format => 'json_gz_format') as j
limit 10;

-- let's count rows for the given file
select
  count(*)
from @lake/gha/2021-04-01-0.json.gz (file_format => 'json_gz_format') as j;

-- ****************************************************************************
-- *** load into tables ***
-- ****************************************************************************
create or replace table "gha-2021-04-01-0__new"
as
  select 
    metadata$filename as __filename,
    metadata$file_row_number as __row_number,
    j.$1 as record
  from @lake/gha/2021-04-01-0.json.gz (file_format => 'json_gz_format') as j;

-- let's count rows for the given file
select
  count(*)
from "gha-2021-04-01-0__new"

describe table "gha-2021-04-01-0"

-- the better way - create table and then use `copy into`
create or replace table gha
(
  record_id number identity,
  __filename varchar(1024),
  __row_number number,
  record variant
);

select count(*) from gha

-- we need some power!
ALTER WAREHOUSE "COMPUTE_WH" 
SET WAREHOUSE_SIZE = 'MEDIUM' 
AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

-- 1479/1488 loaded in 25m5s via MEDIUM wh
-- 194,983,218 records
copy into gha ( __filename, __row_number, record )
from (
  select 
    metadata$filename as __filename,
    metadata$file_row_number as __row_number,
    j.$1 as record
  from @lake/gha/ (
    file_format => 'json_gz_format', 
    pattern=>'.+[.]json[.]gz$'
  ) as j
)
ON_ERROR = SKIP_FILE

-- back to normal
ALTER WAREHOUSE "COMPUTE_WH" 
SET WAREHOUSE_SIZE = 'XSMALL' 
AUTO_SUSPEND = 300 AUTO_RESUME = TRUE;

-- ****************************************************************************
-- *** query unstructured data ***
-- ****************************************************************************

-- How many records are there?
select count(*) from gha;

-- Let's look into "head"
select * from gha limit 100;

select 
  record:"type"::string as record_type,
  count(*) as cnt
from gha
group by record_type
order by cnt desc

select 
  record:"actor":"login"::string as actor_login,
  record:"actor":"url"::string as actor_url,
  count(*) as cnt
from gha
group by actor_login, actor_url
order by cnt desc
limit 10;

select 
  record:"actor":"login"::string as actor_login,
  record:"actor":"url"::string as actor_url,
  count(*) as cnt
from gha
where 
  record:"actor":"login"::string not like '%[bot]'
  and record:"actor":"login"::string not like '%bot'
group by actor_login, actor_url
order by cnt desc
limit 100;
