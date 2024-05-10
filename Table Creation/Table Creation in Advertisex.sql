create or replace table ad_impressions
(user_id string,
ad_creative_id string,
timestamp datetime,
website string
);

create or replace table clicks_conversions
(
user_id string,
ad_campaign_id string,
event_timestamp datetime,
conversion_type string);

create or replace table bids_request
(
user_id string,   
age number,
gender string,
device string,
timestamp datetime,
auction_id string,
ad_format string,
ad_position string, 
ad_rank number, 
bid_amount number,
quality_score number, 
audience_age string,
day_parting string, 
audience_gender string,
location string
);