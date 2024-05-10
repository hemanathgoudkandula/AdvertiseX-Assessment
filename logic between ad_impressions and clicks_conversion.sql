
create or replace table adimpressions_clickconversion
as
select
ai.user_id,ai.* exclude user_id, cc.* exclude user_id
from advertisex.advertisex.ad_impressions ai 
left join advertisex.advertisex.clicks_conversions cc on ai.user_id =cc.user_id;