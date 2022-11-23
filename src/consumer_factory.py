import sys
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING, Producer
import globallogger

logger = globallogger.setup_custom_logger('app')

class ConsumerFactory:

    def get_kafka_connection_arguments(self):
        # Parse the command line.
        parser = ArgumentParser()
        parser.add_argument('config_file', type=FileType('r'))
        parser.add_argument('--reset', action='store_true')
        args = parser.parse_args()
        return args

    def get_kafka_connection_config(self) -> dict:    
        # Parse the configuration.
        # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        config_parser = ConfigParser()
        config_parser.read_file(self.get_kafka_connection_arguments().config_file)
        config = dict(config_parser['default'])
        config.update(config_parser['consumer'])
        return config

    def get_extras_configs(self) -> dict:
        config_parser = ConfigParser()
        config_parser.read_file(self.get_kafka_connection_arguments().config_file)
        return dict(config_parser['consumer_topic'])
    
    def get_topic(self) -> str:
        try:            
            return self.get_extras_configs()["topic_name"]
        except KeyError:
            logger.error(f"You must provide a source topic in the config file. Stoping application.")
            sys.exit()
    
    def get_poll_interval(self) -> float:
        try:
            return float(self.get_extras_configs()["poll_interval"])
        except ValueError:
            return 1.0
    
    def get_name_of_array_field(self) -> str:
        try:            
            return self.get_extras_configs()["name_of_array_field"]
        except KeyError:
            logger.error(f"You must provide a the field containing the array in the config file. Stopping the application.")
            sys.exit()

    def get_subscribed_consumer(self) -> Consumer:
        """Fetch a consumer that is already subscribed according to the config data."""
        # Set up a callback to handle the '--reset' flag.
        consumer = Consumer(self.get_kafka_connection_config())        
        # Subscribe to topic        
        try: 
            consumer.subscribe([self.get_topic()], on_assign=self.reset_offset)
            return consumer
        except Exception as ex:
            logger.error(f"Failed to subscribe to consumer topic. {ex}. Stopping the application.")
            sys.exit()

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(self, consumer, partitions):
        if self.get_kafka_connection_arguments().reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)
        