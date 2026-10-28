import json
import time

import requests
from kafka import KafkaProducer
from kafka.serializer import DefaultSerializer, JsonSerializer

from config import BOOTSTRAP_SERVERS, STREAM_URL, TOPIC
from wiki import parse_sse_line, to_event

HEADERS = {"User-Agent": "kafka-streaming/1.0", "Accept": "text/event-stream"}


def stream_events(last_id: str | None):
    headers = dict(HEADERS)
    if last_id:
        headers["Last-Event-ID"] = last_id  # reprise après une déconnexion
    with requests.get(STREAM_URL, headers=headers, stream=True, timeout=(10, 60)) as resp:
        resp.raise_for_status()
        event_id = None
        for raw_line in resp.iter_lines():
            parsed = parse_sse_line(raw_line.decode("utf-8"))
            if parsed is None:
                continue
            field, value = parsed
            if field == "id":
                event_id = value
            elif field == "data":
                try:
                    yield event_id, json.loads(value)
                except json.JSONDecodeError:
                    continue


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=DefaultSerializer(),
        value_serializer=JsonSerializer(),
        acks="all",
        linger_ms=50,
    )
    print(f"wikimedia -> {TOPIC} ({BOOTSTRAP_SERVERS})", flush=True)
    last_id = None
    sent = 0
    try:
        while True:
            try:
                for event_id, raw in stream_events(last_id):
                    last_id = event_id
                    event = to_event(raw)
                    if event is None:
                        continue
                    # clé = wiki, pour garder l'ordre des modifications d'un même wiki
                    producer.send(TOPIC, key=event["wiki"], value=event)
                    sent += 1
                    if sent % 200 == 0:
                        print(f"{sent} envoyés", flush=True)
            except requests.RequestException as exc:
                print(f"flux coupé ({exc}), reconnexion dans 5s", flush=True)
                time.sleep(5)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
