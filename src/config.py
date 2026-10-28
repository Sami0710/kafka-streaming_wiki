import os

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "wiki.changes")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "wiki-to-postgres")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://wiki:wiki@localhost:5432/wiki")
STREAM_URL = os.getenv("WIKI_STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange")
