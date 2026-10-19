
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "building_sensors_yana"

sensor_id = random.randint(1000, 9999)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(
        value, ensure_ascii=False
    ).encode("utf-8"),
)

print(f"Датчик {sensor_id} запущено. Ctrl+C для зупинки.")

try:
    while True:
        data = {
            "sensor_id": sensor_id,
            "timestamp": datetime.now(
                timezone.utc
            ).isoformat(),
            "temperature": round(random.uniform(10, 45), 2),
            "humidity": round(random.uniform(10, 95), 2),
        }

        producer.send(TOPIC, value=data).get(timeout=10)

        print("Надіслано:", json.dumps(data, ensure_ascii=False))
        time.sleep(2)

except KeyboardInterrupt:
    print("\nРоботу датчика зупинено.")

finally:
    producer.flush()
    producer.close()