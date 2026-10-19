
import json
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"

ALERT_TOPICS = [
    "temperature_alerts_yana",
    "humidity_alerts_yana",
]

consumer = KafkaConsumer(
    *ALERT_TOPICS,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="alerts_display_group",
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),
)

print("Переглядач сповіщень запущено.")
print("Очікування сповіщень... Ctrl+C для зупинки.")

try:
    for message in consumer:
        alert = message.value

        print("\n" + "=" * 50)
        print(f"Топік: {message.topic}")
        print(f"ID датчика: {alert['sensor_id']}")
        print(f"Час: {alert['timestamp']}")
        if "code" in alert:
            print(f"Код алерту: {alert['code']}")
        print(f"Температура: {alert['temperature']}°C")
        print(f"Вологість: {alert['humidity']}%")
        print(f"Повідомлення: {alert['message']}")
        print("=" * 50)

except KeyboardInterrupt:
    print("\nПереглядач зупинено.")

finally:
    consumer.close()