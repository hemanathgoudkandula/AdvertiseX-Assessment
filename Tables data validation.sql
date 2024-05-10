-- ad impressions data validation
select 
user_id, count(*) as cnt
from advertisex.advertisex.ad_impressions
group by user_id;

-- clicks conversion data validation
select
user_id,ad_campaign_id, count(*) as cnt
from advertisex.advertisex.clicks_conversions
group by user_id,ad_campaign_id;

-- bids requests data validation
select 
user_id,auction_id,count(*) as cnt
from advertisex.advertisex.bids_request
group by user_id,auction_id;