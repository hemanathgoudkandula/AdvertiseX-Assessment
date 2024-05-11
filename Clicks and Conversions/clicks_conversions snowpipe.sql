-- file format creation
create or replace file format clicks_conversions_format
type = CSV
FIELD_DELIMITER =','
SKIP_HEADER =1;

-- staging table creation
create or replace stage ADVERTISEX.STAGING.CLICKS_CONVERSIONS
file_format = clicks_conversions_format
url ='s3://advertisex/clicks_conversions/'
STORAGE_INTEGRATION = advertisex_s3;

-- select columns query
select $2::string as user_id,$3::string as ad_campaign_id,$1::datetime as event_timestamp,$4::string as conversion_type from  @ADVERTISEX.STAGING.CLICKS_CONVERSIONS;

-- loading data to stage table query
copy into ADVERTISEX.STAGING.CLICKS_CONVERSIONS_STAGE
(user_id,ad_campaign_id,event_timestamp,conversion_type)
from (select $2::string as user_id,$3::string as ad_campaign_id,$1::datetime as event_timestamp,$4::string as conversion_type from  @ADVERTISEX.STAGING.CLICKS_CONVERSIONS)
on_error = 'skip_file'
; 

-- snow pipe creation for clicks_conversions

create or replace pipe clicks_conversion_pipe
AUTO_INGEST = TRUE
as 
copy into ADVERTISEX.STAGING.CLICKS_CONVERSIONS_STAGE
(user_id,ad_campaign_id,event_timestamp,conversion_type)
from (select $2::string as user_id,$3::string as ad_campaign_id,$1::datetime as event_timestamp,$4::string as conversion_type from  @ADVERTISEX.STAGING.CLICKS_CONVERSIONS)
on_error = 'skip_file'
;

-- store procedure for merging the data 

create or replace PROCEDURE clicks_conversion_procedure()
returns string
language sql
as
begin
merge into ADVERTISEX.ADVERTISEX.clicks_conversions cc
using (select *
from advertisex.staging.clicks_conversions_stage
qualify row_number() over(partition by user_id,ad_campaign_id order by user_id) =1) ccs 
on cc.user_id = ccs.user_id and cc.ad_campaign_id =ccs.ad_campaign_id
when not matched then 
insert (user_id,ad_campaign_id ,event_timestamp ,conversion_type ) values (user_id,ad_campaign_id ,event_timestamp ,conversion_type );

 RETURN 'Upsert operation completed successfully';
end
;

-- task creation for incremental on hourly basis for clicks_conversion

create or replace task clicks_conversions_task
schedule ='60 minute'
SUSPEND_TASK_AFTER_NUM_FAILURES =2
TASK_AUTO_RETRY_ATTEMPTS =2
as 
call clicks_conversion_procedure();

