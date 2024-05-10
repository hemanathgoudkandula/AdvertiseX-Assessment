from faker import Faker
from datetime import datetime,date
from fastavro import writer, parse_schema
import random
import json

schema = {
    "type": "record",
    "name": "BidRequest",
    "fields": [
        {
            "name": "user_details",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "user_info",
                    "fields": [
                        {"name": "user_id", "type": "string"},
                        {"name": "age", "type": "int"},
                        {"name": "gender", "type": "string"},
                        {"name": "device", "type": "string"}
                    ]
                }
            }
        },
        {
            "name": "auction_details",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "auction_info",
                    "fields": [
                        {"name":"auction_id","type" :"string"},
                        {"name": "ad_rank", "type": "int"},
                        {"name": "quality_score", "type": "int"},
                        {"name": "bid_amount", "type": "int"},
                        {"name": "ad_format", "type": "string"},
                        {"name": "ad_position", "type": "string"}
                    ]
                }
            }
        },
        {"name": "timestamp", "type": "string"},
        {
            "name": "targeting_criteria",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "targeting_info",
                    "fields": [
                        {"name": "location", "type": "string"},
                        {
                            "name": "audience_targeting",
                            "type": {
                                "type": "array",
                                "items": {
                                    "type": "record",
                                    "name": "audience_info",
                                    "fields": [
                                        {"name": "age", "type": "string"},
                                        {"name": "gender", "type": "string"},
                                        {"name": "day_parting", "type": "string"}
                                    ]
                                }
                            }
                        }
                    ]
                }
            }
        }
    ]
}

fake = Faker()

file_name = open('ad_impressions_'+str(date.today())+'.json')

data = json.load(file_name)

fixed_age_gender = {}
for user_data in data:
    user_id = user_data['user_id']
    fixed_age_gender[user_id] = {
        "age": random.randint(18, 60),
        "gender": random.choice(["male", "female"])
    }

bid_requests = []

for i in range(1, 10000):
    
    if random.uniform(0,1) <=0.3 and i!=1:
        
        temp_item = random.choice(bid_requests) 
        temp_user_details = temp_item["user_details"]
        temp_auction_details = temp_item["auction_details"]
        temp_audience_targeting = temp_item["targeting_criteria"]
        temp_audience_targeting_criteria =temp_audience_targeting[0]["audience_targeting"]
        user_id1 = temp_user_details[0]["user_id"]
        age1 = temp_user_details[0]["age"]
        gender1 = temp_user_details[0]["gender"]    
        device = temp_user_details[0]["device"]
        auction_id = temp_auction_details[0]["auction_id"]
        ad_rank = temp_auction_details[0]["ad_rank"]
        quality_score = temp_auction_details[0]["quality_score"]
        bid_amount = temp_auction_details[0]["bid_amount"]
        ad_format = temp_auction_details[0]["ad_format"]
        ad_position = temp_auction_details[0]["ad_position"]
        timestamp = temp_item["timestamp"]
        location = temp_audience_targeting[0]["location"]
        audience_targeting_age = temp_audience_targeting_criteria[0]["age"]
        audience_targeting_gender = temp_audience_targeting_criteria[0]["gender"]
        audience_targeting_dayparting = temp_audience_targeting_criteria[0]["day_parting"]

        item = {
        "user_details": [{"user_id": user_id1,
                        "age": age1,
                        "gender": gender1,
                        "device": device
                        }],
        "auction_details": [{"auction_id":auction_id,
                            "ad_rank": ad_rank,
                            "quality_score": quality_score,
                            "bid_amount": bid_amount,
                            "ad_format": ad_format,
                            "ad_position": ad_position
                            }],
        "timestamp": timestamp,
        "targeting_criteria": [{"location": location,
                                "audience_targeting": [{"age": audience_targeting_age,
                                                        "gender": audience_targeting_gender,
                                                        "day_parting": audience_targeting_dayparting
                                                        }]
                            }]
    }
    else:
        user_data = random.choice(data)
        user_id = user_data['user_id']
        age = fixed_age_gender[user_id]["age"]
        gender = fixed_age_gender[user_id]["gender"]
        item = {
            "user_details": [{"user_id": user_id,
                            "age": age,
                            "gender": gender,
                            "device": random.choice(["desktop", "laptop", "tablet", "mobile"])}],
            "auction_details": [{"auction_id":fake.bothify(text ='auc-######'),
                                "ad_rank": random.randint(1, 20),
                                "quality_score": random.randint(1, 10),
                                "bid_amount": random.randint(1, 10),
                                "ad_format": random.choice(["text", "image", "video"]),
                                "ad_position": random.choice(["top", "bottom", "center", "sidebar"])}],
            "timestamp": fake.date_time_between_dates(datetime_start=datetime(2024, 1, 1),
                                                    datetime_end=date.today()).isoformat(),
            "targeting_criteria": [{"location": fake.country(),
                                    "audience_targeting": [{"age": random.choice(['13-17', '18-24', '25-34', '35-44', '45-54', '55-64', '65+']),
                                                            "gender": random.choice(["male", "female"]),
                                                            "day_parting": random.choice(["Morning", "Afternoon", "Evening", "Night", "24 Hrs"])}]
                                }]
        }
    bid_requests.append(item)

#  Save to an Avro file
parsed_schema = parse_schema(schema)
with open('bid_requests_'+str(date.today())+'.avro', 'wb') as f:
    writer(f, parsed_schema, bid_requests)
