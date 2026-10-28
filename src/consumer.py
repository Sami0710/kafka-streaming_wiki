import psycopg
from kafka import KafkaConsumer
from kafka.serializer import JsonSerializer

from config import BOOTSTRAP_SERVERS, DATABASE_URL, GROUP_ID, TOPIC

INSERT_SQL = """
    INSERT INTO wiki_changes (id, ts, wiki, title, user_name, is_bot, change_type, size_delta)
    VALUES (%(id)s, %(ts)s, %(wiki)s, %(title)s, %(user)s, %(bot)s, %(type)s, %(size_delta)s)
    ON CONFLICT (id) DO NOTHING
"""


def main() -> None:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=JsonSerializer(),
    )
    print(f"{TOPIC} -> postgres (groupe {GROUP_ID})", flush=True)
    total = 0
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            while True:
                batches = consumer.poll(timeout_ms=1000, max_records=500)
                events = [record.value for records in batches.values() for record in records]
                if not events:
                    continue
                with conn.cursor() as cur:
                    cur.executemany(INSERT_SQL, events)
                conn.commit()
                # commit Kafka après l'écriture en base : en cas de crash les messages sont relus,
                # et ON CONFLICT évite les doublons
                consumer.commit()
                total += len(events)
                print(f"+{len(events)} (total {total})", flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
