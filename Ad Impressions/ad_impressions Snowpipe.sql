-- file format creation
create or replace file format ad_impressions_format
type = JSON
STRIP_OUTER_ARRAY =TRUE
;

-- staging table creation
create or replace stage ADVERTISEX.STAGING.AD_IMPRESSIONS
file_format = ad_impressions_format
url ='s3://advertisex/ad_impressions/'
STORAGE_INTEGRATION = advertisex_s3;

-- select columns query
select $1:user_id::string as user_id, $1:ad_creative_id::string as ad_creative_id,$1:timestamp::datetime as timestamp ,$1:website::string as website from @ADVERTISEX.STAGING.AD_IMPRESSIONS;

-- loading data to stage table query
copy into ADVERTISEX.STAGING.AD_IMPRESSIONS_STAGE
(user_id,ad_creative_id, timestamp, website)
from 
(select $1:user_id::string as user_id,$1:ad_creative_id::string as ad_creative_id,$1:timestamp::datetime as timestamp ,$1:website::string as website 
from @ADVERTISEX.STAGING.AD_IMPRESSIONS)
on_error = 'skip_file';

-- snowpipe creation for ad impressions
create or replace pipe ad_impression_pipe
AUTO_INGEST = TRUE
as
copy into ADVERTISEX.STAGING.AD_IMPRESSIONS_STAGE
(user_id,ad_creative_id, timestamp, website)
from 
(select $1:user_id::string as user_id,$1:ad_creative_id::string as ad_creative_id,$1:timestamp::datetime as timestamp ,$1:website::string as website 
from @ADVERTISEX.STAGING.AD_IMPRESSIONS)
on_error = 'skip_file';


-- store procedure for merging the data

create or replace PROCEDURE ad_impressions_procedure()
returns string
language sql
as
begin
merge into ADVERTISEX.ADVERTISEX.AD_IMPRESSIONS ad
using (select * from ADVERTISEX.STAGING.AD_IMPRESSIONS_STAGE  qualify row_number() over(partition by user_id order by user_id )=1) ads on ad.user_id = ads.user_id
when not matched then 
insert (user_id,ad_creative_id ,timestamp ,website ) values (user_id,ad_creative_id ,timestamp ,website );

 RETURN 'Upsert operation completed successfully';
end
;

-- task creation for incremental on hourly basis for ad_impressions

create or replace task ad_impressions_task
schedule ='60 minute'
SUSPEND_TASK_AFTER_NUM_FAILURES =2
TASK_AUTO_RETRY_ATTEMPTS =2
as 
call ad_impressions_procedure();
