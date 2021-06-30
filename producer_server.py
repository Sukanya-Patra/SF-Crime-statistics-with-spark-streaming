from kafka import KafkaProducer
import json
import time
import logging

logger = logging.getLogger(__name__)


class ProducerServer(KafkaProducer):
    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    def generate_data(self):
        with open(self.input_file) as f:
            lines = json.load(f)

            for line in lines:  # originally f

                message = self.dict_to_binary(line)
                logger.info(f"message:{message}")

                self.send(topic=self.topic, value=message)
                time.sleep(1)

    def dict_to_binary(self, json_dict):
        binary = json.dumps(json_dict).encode("utf-8")
        return binary
