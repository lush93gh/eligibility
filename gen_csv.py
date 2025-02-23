import csv
import random
from datetime import datetime, timedelta

# Function to generate random timestamps
def generate_timestamp(start_date):
    end_date = start_date + timedelta(days=7)
    random_date = start_date + (end_date - start_date) * random.random()
    return random_date.strftime("%Y-%m-%d %H:%M:%S UTC")

# Define the schema
headers = ["user_pseudo_id", "subscribe_timestamp", "subscribe_day_in_first_seven_days", "session_id", "event_name", "event_timestamp", "session_number"]

# Define the event names, there are 10 in total
event_names = ["level_up", "collect_gem", "chest_open", "coin_get", "kill_monster", "npc_talk", "new_map", "new_partner", "info_obtained", "time_travel"]

# Start date for subscription, assuming the script is run on the day of subscription
start_date = datetime.utcnow()

# Generate data for 15 unique users
data = []
for user_id in range(1, 16):
    subscribe_timestamp = generate_timestamp(start_date)
    for session_number in range(random.randint(1, 10)):
        topFiveWeights = [55, 34, 21, 13, 8]
        random.shuffle(topFiveWeights)
        weights = topFiveWeights + [5, 3, 2, 1, 1]
        count = 0
        for event in random.choices(event_names, weights = weights, k=random.randint(20, 50)):
            data.append([
                f"user_{user_id:03d}",
                subscribe_timestamp,
                "True" if (datetime.utcnow() - datetime.strptime(subscribe_timestamp, "%Y-%m-%d %H:%M:%S UTC")).days < 7 else "False",
                f"session_{user_id:03d}_{session_number:03d}",
                event ,
                generate_timestamp(start_date),
                session_number
            ])
            count += 1

# Write data to CSV
with open('before_sub_events.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(headers)
    writer.writerows(data)

print("CSV file 'before_sub_events.csv' has been generated.")
