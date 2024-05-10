-- file format creation
create or replace file format bids_request_format
type = AVRO
TRIM_SPACE = TRUE
;

-- staging table creation
create or replace stage ADVERTISEX.STAGING.BID_REQUESTS
file_format = bids_request_format
url ='s3://advertisex/bid_requests/';

-- select columns query
select
$1:user_details[0]:user_id::string as user_id,   
$1:user_details[0]:age::number as user_age,
$1:user_details[0]:gender::string as user_gender,
$1:user_details[0]:device::string as device,
$1:timestamp::datetime as timestamp,
$1:auction_details[0]:auction_id::string as auction_id,
$1:auction_details[0]:ad_format::string as ad_format,
$1:auction_details[0]:ad_position::string as ad_position, 
$1:auction_details[0]:ad_rank::number as ad_rank, 
$1:auction_details[0]:bid_amount::number as bid_amount,
$1:auction_details[0]:quality_score::number as quality_score, 
$1:targeting_criteria[0]:audience_targeting[0]:age::string as audience_age,
$1:targeting_criteria[0]:audience_targeting[0]:day_parting::string as day_parting, 
$1:targeting_criteria[0]:audience_targeting[0]:gender::string as audience_gender,
$1:targeting_criteria[0]:location::string as location 
from @ADVERTISEX.STAGING.BID_REQUESTS
;

-- loading data to stage table query
copy into ADVERTISEX.STAGING.BIDS_REQUEST_STAGE
(user_id,age,gender,device,timestamp,auction_id,ad_format,ad_position,
ad_rank,bid_amount,quality_score,audience_age,day_parting,audience_gender,location)
from 
(select
$1:user_details[0]:user_id::string as user_id,   
$1:user_details[0]:age::number as user_age,
$1:user_details[0]:gender::string as user_gender,
$1:user_details[0]:device::string as device,
$1:timestamp::datetime as timestamp,
$1:auction_details[0]:auction_id::string as auction_id,
$1:auction_details[0]:ad_format::string as ad_format,
$1:auction_details[0]:ad_position::string as ad_position, 
$1:auction_details[0]:ad_rank::number as ad_rank, 
$1:auction_details[0]:bid_amount::number as bid_amount,
$1:auction_details[0]:quality_score::number as quality_score, 
$1:targeting_criteria[0]:audience_targeting[0]:age::string as audience_age,
$1:targeting_criteria[0]:audience_targeting[0]:day_parting::string as day_parting, 
$1:targeting_criteria[0]:audience_targeting[0]:gender::string as audience_gender,
$1:targeting_criteria[0]:location::string as location 
from @ADVERTISEX.STAGING.BID_REQUESTS)
on_error = 'skip_file';


-- snow pipe creation for bids requests

create or replace pipe bids_requests_pipe
AUTO_INGEST = TRUE
as
copy into ADVERTISEX.STAGING.BIDS_REQUEST_STAGE
(user_id,age,gender,device,timestamp,auction_id,ad_format,ad_position,
ad_rank,bid_amount,quality_score,audience_age,day_parting,audience_gender,location)
from 
(select
$1:user_details[0]:user_id::string as user_id,   
$1:user_details[0]:age::number as user_age,
$1:user_details[0]:gender::string as user_gender,
$1:user_details[0]:device::string as device,
$1:timestamp::datetime as timestamp,
$1:auction_details[0]:auction_id::string as auction_id,
$1:auction_details[0]:ad_format::string as ad_format,
$1:auction_details[0]:ad_position::string as ad_position, 
$1:auction_details[0]:ad_rank::number as ad_rank, 
$1:auction_details[0]:bid_amount::number as bid_amount,
$1:auction_details[0]:quality_score::number as quality_score, 
$1:targeting_criteria[0]:audience_targeting[0]:age::string as audience_age,
$1:targeting_criteria[0]:audience_targeting[0]:day_parting::string as day_parting, 
$1:targeting_criteria[0]:audience_targeting[0]:gender::string as audience_gender,
$1:targeting_criteria[0]:location::string as location 
from @ADVERTISEX.STAGING.BID_REQUESTS)
on_error = 'skip_file';


-- store procedure for merging the data

create or replace procedure bids_request_procedure()
returns string
language sql
as
begin
merge into ADVERTISEX.ADVERTISEX.bids_request br
using (
select *
from advertisex.staging.bids_request_stage
qualify row_number() over(partition by user_id,auction_id order by user_id) =1) brs 
on br.user_id = brs.user_id and br.AUCTION_ID =brs.AUCTION_ID
when not matched then 
insert (USER_ID, AGE, GENDER, DEVICE, TIMESTAMP, AUCTION_ID, AD_FORMAT, AD_POSITION, AD_RANK, BID_AMOUNT, QUALITY_SCORE, AUDIENCE_AGE, DAY_PARTING, AUDIENCE_GENDER, LOCATION) values (USER_ID, AGE, GENDER, DEVICE, TIMESTAMP, AUCTION_ID, AD_FORMAT, AD_POSITION, AD_RANK, BID_AMOUNT, QUALITY_SCORE, AUDIENCE_AGE, DAY_PARTING, AUDIENCE_GENDER, LOCATION);

RETURN 'Upsert operation completed successfully';
end
;

-- task creation for incremental on hourly basis for bids_request
create or replace task bids_request_task
schedule ='60 minute'
SUSPEND_TASK_AFTER_NUM_FAILURES =2
TASK_AUTO_RETRY_ATTEMPTS =2
as 
call bids_request_procedure();