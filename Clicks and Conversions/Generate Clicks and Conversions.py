from faker import Faker
from datetime import datetime,date
import csv
import random
import json

fake = Faker()

file_name = open('ad_impressions_'+str(date.today())+'.json')

data = json.load(file_name)

clicks_conversions=[]

for i in range(1,10000):
    if random.uniform(0,1) <= 0.3 and i != 1:
        temp_item = random.choice(clicks_conversions) 
        event_timestamp = temp_item[0]
        user_id = temp_item[1]
        ad_campaign_id = temp_item[2]
        conversion_type = temp_item[3]

        item = [
            event_timestamp,
            user_id,
            ad_campaign_id,
            conversion_type
            ]
    else:
        item = [
            
            fake.date_time_between_dates(datetime_start= datetime(2024,1,1), datetime_end =date.today()).isoformat(),
            random.choice(data)["user_id"],
            fake.bothify(text ='ad-######'),
            random.choice(["click_ad","view_ad","view_content","sign_up"])
            ]
    clicks_conversions.append(item)


with open('clicks_conversions_'+str(date.today())+'.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["event_timestamp", "user_id", "ad_campaign_id", "conversion_type"])
    writer.writerows(clicks_conversions)
