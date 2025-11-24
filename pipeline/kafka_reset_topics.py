#!/usr/bin/env python3
"""Delete and recreate Kafka topics for a clean slate.

Requires kafka-python. Operates on localhost:9092 by default.
"""
import argparse
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


def reset_topics(bootstrap_servers: str, topics: list[str], partitions: int = 3, replication_factor: int = 1):
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="reset-topics")
    # Delete if exist
    try:
        admin.delete_topics(topics)
    except Exception:
        pass
    # Wait briefly for deletion to propagate
    import time
    time.sleep(2)
    # Recreate
    new_topics = [NewTopic(name=t, num_partitions=partitions, replication_factor=replication_factor) for t in topics]
    try:
        admin.create_topics(new_topics=new_topics, validate_only=False)
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--partitions", type=int, default=3)
    p.add_argument("--replication", type=int, default=1)
    p.add_argument("topics", nargs="+", default=["iot-metrics"])
    args = p.parse_args()
    reset_topics(args.bootstrap, args.topics, args.partitions, args.replication)