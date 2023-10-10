import pika
import json
import logging
import time
from configparser import ConfigParser
from .load import Load

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class RabbitMQConsumer:

    def __init__(self, config_file):
        
        self.config = self._get_rabbitmq_config(config_file)
        self.connection = pika.BlockingConnection(self.config["connection_parameters"])
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.config["rabbit_queue"], durable=True)
        self.channel.basic_qos(prefetch_count=1)

    def _get_rabbitmq_config(self, config_file):
        config = ConfigParser()
        config.read([config_file])
        
        credentials = pika.PlainCredentials(
            config['RABBITMQ']['RABBIT_USER'], config['RABBITMQ']['RABBIT_PASS']
        )
        connection_parameters = pika.ConnectionParameters(
            config['RABBITMQ']['RABBIT_HOST'],
            config['RABBITMQ']['RABBIT_PORT'],
            config['RABBITMQ']['RABBIT_VHOST'],
            credentials
        )
        return {
            "connection_parameters": connection_parameters,
            "rabbit_queue": config['RABBITMQ']['RABBIT_READ_FROM']
        }

    def callback(self, ch, method, properties, body):
        try:

            decoded_str = body.decode('utf-8')

            str_without_escapes = json.loads(decoded_str)

            final_dict = json.loads(str_without_escapes)

            db = Load(host="localhost", user="root", password="", database="mobi")

            db.insert_data('raw_book', final_dict)
            db.close()
            
            logging.info(f"Processed message: {body}")
            
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logging.error(f"Error processing message. Reason: {e}")

    def consume(self):
        self.channel.basic_consume(queue=self.config["rabbit_queue"], on_message_callback=self.callback)
        logging.info("Starting to consume messages...")
        self.channel.start_consuming()

    def process_message(message):
        some_value = message["some_key"]