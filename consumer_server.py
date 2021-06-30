from confluent_kafka import Consumer
import asyncio
import logging

logger = logging.getLogger(__name__)


BROKER_URL = "localhost:9092"
TOPIC_NAME = "sf_crimes"


async def consume(topic_name):
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": 0})
    c.subscribe([topic_name])

    while True:
        messages = c.consume(5, timeout=1.0)
        for message in messages:
            logger.info(f"consumed message: {message.value()}")


def main():
    try:
        asyncio.run(consume(TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    main()
