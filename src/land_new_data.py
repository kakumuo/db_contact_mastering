import random
import names_generator as ng
import random_address as ra
from datetime import datetime
import json
from utils.utils import get_config

config = get_config()
target_volume_path = config.get("target_volume_path")

def generate_phone_number(opt): 
    area = str(random.randint(0,999)).zfill(3)
    exch = str(random.randint(0,999)).zfill(3)
    line = str(random.randint(0,9999)).zfill(4)
    
    if opt & 1<<0: area = f"({area})"
    if opt & 1<<1: exch = f" {exch} "
    if opt & 1<<2: exch = f"-{exch}-"

    return "".join([area, exch, line])

def generate_contact(): 
    data = {
        "email": None, 
        "phone": None, 
        "preferred_contact_type": None, 
    }

    contact_types = ["Email", "Phone"]

    match (random.randint(0,2)): 
        case 0: 
            data["email"] = f"{ng.generate_name()}@example.com"
            data["preferred_contact_type"] = contact_types[0]
        case 1: 
            data["phone"] = generate_phone_number(random.randint(0,8))
            data["preferred_contact_type"] = contact_types[1]
        case 2: 
            data["email"] = f"{ng.generate_name()}@example.com"
            data["phone"] = generate_phone_number(random.randint(0,8))
            data["preferred_contact_type"] = random.choice(contact_types)
    return data

def generate_preferences(): 
    return {
        "new_product": random.choice([True, False]),
        "educational": random.choice([True, False]),
        "discounts": random.choice([True, False]),
        "news": random.choice([True, False]),
        "insights": random.choice([True, False])
    }


def generate_address(addr_type:str): 
    addr = ra.real_random_address()

    state_code_to_text = {"AL":"Alabama","AK":"Alaska","AS":"American Samoa","AZ":"Arizona","AR":"Arkansas","CA":"California","CO":"Colorado","CT":"Connecticut","DE":"Delaware","DC":"Dist. of Columbia","FL":"Florida","GA":"Georgia","GU":"Guam","HI":"Hawaii","ID":"Idaho","IL":"Illinois","IN":"Indiana","IA":"Iowa","KS":"Kansas","KY":"Kentucky","LA":"Louisiana","ME":"Maine","MD":"Maryland","MH":"Marshall Islands","MA":"Massachusetts","MI":"Michigan","FM":"Micronesia","MN":"Minnesota","MS":"Mississippi","MO":"Missouri","MT":"Montana","NE":"Nebraska","NV":"Nevada","NH":"New Hampshire","NJ":"New Jersey","NM":"New Mexico","NY":"New York","NC":"North Carolina","ND":"North Dakota","MP":"Northern Marianas","OH":"Ohio","OK":"Oklahoma","OR":"Oregon","PW":"Palau","PA":"Pennsylvania","PR":"Puerto Rico","RI":"Rhode Island","SC":"South Carolina","SD":"South Dakota","TN":"Tennessee","TX":"Texas","UT":"Utah","VT":"Vermont","VA":"Virginia","VI":"Virgin Islands","WA":"Washington","WV":"West Virginia","WI":"Wisconsin","WY":"Wyoming"}

    return {
        "address_line_1": addr["address1"],
        "address_line_2": None if addr["address2"] == "" else addr["address2"], 
        "address_type": addr_type,
        "city": addr["city"],
        "state": state_code_to_text[addr["state"]],
        "zip": addr["postal_code"],
        "country": "United States"
    }
    pass

def generate_user(uname:str):
    contact_info = generate_contact()
    return {
        "username": uname,
        "email": contact_info["email"],
        "phone": contact_info["phone"],
        "preferred_contact_type": contact_info["preferred_contact_type"],
        "first_name": random.choice(["John", "Johnny", "Jane", "Bob", "Bobby", "Alice", "Mikey", "Robert", "Emma", "David", "Sophia"]),
        "last_name": random.choice(["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson"]),
        "preferences": generate_preferences(),
        "address": [generate_address(addr_type) for addr_type in random.sample(["Shipping", "Billing", "Mailing", "Physical"], random.randint(1,3))],
        "updated_at": datetime.now().isoformat()
    }

def generate_delete(uname:str):     
    sentences = [
        "The service isn't meeting my needs.",
        "Customer service is unhelpful.",
        "I'm concerned about data security.",
        "The cost is too high.",
        "Terms and conditions have changed.",
        "Features are lacking or unhelpful.",
        "I've found a better alternative.",
        "There are too many technical issues.",
        "I don't have control over my account.",
        "I've lost trust in the service.",
        "I get too much unwanted email.",
        "I no longer need the service.",
        "My personal circumstances have changed.",
        "Competitors offer better options.",
        "I'm concerned about the company's values."
    ]

    return {
        "username": uname, 
        "reason":" ".join(random.sample(sentences, random.randint(1,4))), 
        "requested_at":datetime.now().isoformat()
    }

# generate user info and deletion info
count = 10
usernames = [ng.generate_name("underscore") for i in range(count)]

user_data = [generate_user(uname) for uname in usernames]
delete_data = [generate_delete(uname) for uname in usernames[:3]]

data_files = [
    ("user", user_data), 
    ("delete_request", delete_data)
]

for file_prefix, file_data in data_files:
    with open(f"{target_volume_path}/{file_prefix}_{datetime.now():%Y%m%d%H%M%S}.json", "w") as jsonFile:
        json.dump(file_data, jsonFile, indent=4)

# spark.createDataFrame(user_data).repartition(1).write.json(f"{target_volume_path}/user_{datetime.now():%Y%m%d%H%M%S}")
# spark.createDataFrame(delete_data).repartition(1).write.json(f"{target_volume_path}/delete_requests_{datetime.now():%Y%m%d%H%M%S}")
