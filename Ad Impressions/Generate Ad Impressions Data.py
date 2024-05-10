from faker import Faker
from datetime import datetime,date
import json
import random

fake = Faker()

ad_impressions=[]

for i in range(1,1000):
    if random.uniform(0,1) <= 0.3 and i != 1:

        temp_item = random.choice(ad_impressions) 
        user_id = temp_item["user_id"]
        ad_creative_id = temp_item["ad_creative_id"]
        website = temp_item["website"]
        timestamp = temp_item["timestamp"]
        
        item = {
            "ad_creative_id": ad_creative_id,
            "user_id": user_id,
            "timestamp":timestamp,
            "website": website
        }
    else:
        item = {
            "ad_creative_id": fake.bothify(text ='C-######'),
            "user_id": fake.bothify(text ='###-###-####'),
            "timestamp":fake.date_time_between_dates(datetime_start= datetime(2024,1,1), datetime_end =date.today()).isoformat(),
            "website": fake.url()
        }

    ad_impressions.append(item)
    


with open('ad_impressions_'+str(date.today())+'.json', 'w') as f:
    json.dump(ad_impressions, f, indent=4)
