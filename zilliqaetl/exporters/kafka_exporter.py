import os
from confluent_kafka import Producer, KafkaException
from timeout_decorator import timeout_decorator

import logging

import socket
import json


class KafkaItemExporter:
    def __init__(self, item_type_to_topic_mapping, message_attributes=("item_id",)) -> None:
        logging.basicConfig(
            level=logging.INFO, filename="message-publish.log",
            format='{"time" : "%(asctime)s", "level" : "%(levelname)s" , "message" : "%(message)s"}',
        )

        conf = {
            "bootstrap.servers": os.getenv("CONFLUENT_BROKER"),
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "client.id": socket.gethostname(),
            "message.max.bytes": 5242880,
            "sasl.username": os.getenv("KAFKA_PRODUCER_KEY"),
            "sasl.password": os.getenv("KAFKA_PRODUCER_PASSWORD")
        }

        producer = Producer(conf)
        self.item_type_to_topic_mapping = item_type_to_topic_mapping
        self.producer = producer
        self.logging = logging.getLogger(__name__)
        self.message_attributes = message_attributes

    def open(self):
        pass

    def export_items(self, items):
        try:
            self._export_items_with_timeout(items)
        except timeout_decorator.TimeoutError as e:
            logging.error("Timeout error")
            raise e

    @timeout_decorator.timeout(300)
    def _export_items_with_timeout(self, items):
        for item in items:
            self.export_item(item)

    def export_item(self, item):
        item_type = item.get("type", None)
        has_item_type = item_type is not None
        if has_item_type and item_type in self.item_type_to_topic_mapping:
            topic = self.item_type_to_topic_mapping[item_type]
            data = json.dumps(item).encode("utf-8")
            self.write_txns(key=item.get("token_address"), value=data.decode("utf-8"), topic=topic)
        else:
            logging.error(f'Topic for item type {item_type} is not configured.')

    def get_message_attributes(self, item):
        attributes = {}

        for attr_name in self.message_attributes:
            if item.get(attr_name) is not None:
                attributes[attr_name] = item.get(attr_name)

        return attributes

    def close(self):
        self.producer.flush()
        pass

    def write_txns(self, key: str, value: str, topic: str):
        # def acked(err, msg):
        #     if err is not None:
        #         self.logging.error(f' Message failed delivery, {topic} : {err}\n')

        try:
            self.producer.produce(topic, key=key, value=value)
        except BufferError:
            self.logging.error('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                               len(self.producer))
        except KafkaException as e:
            self.logging.error(f"Kafka Exception for topic : {topic} , exception : {e}")
        except NotImplementedError as e:
            self.logging.error(f"NotImplementedError for topic : {topic} , exception : {e}")
        self.producer.poll(0)
