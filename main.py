import logging
from api.consumer import RabbitMQConsumer

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# configurações
config_path = './config/config.ini'

consumer = RabbitMQConsumer(config_path)

consumer.consume()
