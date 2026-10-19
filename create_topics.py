
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP_SERVERS = "localhost:9092"

TOPICS = [
    "building_sensors_yana",
    "temperature_alerts_yana",
    "humidity_alerts_yana",
]


def main():
    admin = KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="topic_creator",
    )

    try:
        existing_topics = admin.list_topics()

        new_topics = [
            NewTopic(
                name=name,
                num_partitions=1,
                replication_factor=1,
            )
            for name in TOPICS
            if name not in existing_topics
        ]

        if new_topics:
            admin.create_topics(
                new_topics=new_topics,
                validate_only=False,
            )
            print("Топіки успішно створено:")
            for topic in new_topics:
                print(f" - {topic.name}")
        else:
            print("Усі топіки вже існують.")

    finally:
        admin.close()


if __name__ == "__main__":
    main()