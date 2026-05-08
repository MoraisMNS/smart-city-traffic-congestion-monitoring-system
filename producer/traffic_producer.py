from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

junctions = [
    "Pettah",
    "Rajagiriya",
    "Nugegoda",
    "Bambalapitiya"
]

while True:

    junction = random.choice(junctions)

    # Occasionally generate critical traffic
    if random.random() < 0.1:
        avg_speed = random.uniform(3, 9)
        vehicle_count = random.randint(80, 120)
    else:
        avg_speed = random.uniform(20, 60)
        vehicle_count = random.randint(10, 70)

    data = {
        "sensor_id": junction,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "vehicle_count": vehicle_count,
        "avg_speed": round(avg_speed, 2)
    }

    producer.send('traffic-data', value=data)

    print("Sent:", data)

    time.sleep(1)