-- Joining both ad_impressions and Clicks_conversion
create or replace table adimpressions_clickconversion
as
select
ai.user_id, ai.ad_creative_id,ai.website,cc.ad_campaign_id,cc.event_timestamp,cc.conversion_type
from advertisex.advertisex.ad_impressions ai 
left join advertisex.advertisex.clicks_conversions cc on ai.user_id =cc.user_id;

-- For columns which are selected from ad_impressions and Clicks_conversion
select user_id,ad_creative_id,website,ad_campaign_id,event_timestamp,conversion_type from adimpressions_clickconversion;
