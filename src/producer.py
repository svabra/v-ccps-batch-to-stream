import sys
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING, Producer as KProducer
import json
from jsonpath_ng import jsonpath, parse
# from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
# from confluent_kafka.schema_registry import SchemaRegistryClient
# from confluent_kafka.schema_registry.avro import AvroSerializer
import globallogger

logger = globallogger.setup_custom_logger('app')

class Producer:
    def __init__(self) -> None:
         # Create Producer instance
        self.producer = KProducer(self.get_kafka_connection_config())
        self.topic = self.get_topic()

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
        config.update(config_parser['producer'])
        print(config)
        return config

    def get_extras_configs(self) -> dict:
        config_parser = ConfigParser()
        config_parser.read_file(self.get_kafka_connection_arguments().config_file)
        return dict(config_parser['producer_topic'])
    
    def get_topic(self) -> str:
        try:            
            return self.get_extras_configs()["topic_name"]
        except KeyError:
            logger.error(f"You must provide a sink topic in the config file. Stoping application.")
            sys.exit()
    
    def get_schema_registry_url(self) -> str:
        try:            
            return self.get_extras_configs()["producer_topic"]
        except KeyError:
            logger.error(f"You must provide a schema_registry_url in the config file in order to comply with the AVRO schema. Stoping application.")
            sys.exit()

    def get_key_path(self) -> str:
        """Return the path of the key in the source events, which should be copied to the sink events."""
        try:            
            return self.get_extras_configs()["key_path_in_source_topic"]
        except KeyError:
            logger.debug(f"There is no key_path_in_source_topic in the config, thus we assume no key provided programmaticaly.")
            return None   

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(self, err, msg):
        if err:
            # This is unlikely to happen, but it could result in data loss. Thus we shut down the processor.
            logger.error('ERROR: Message failed delivery: {}. The processor is shutting down. '.format(err))
            sys.exit()
        else:
            logger.debug(f"Produced event to topic {msg.topic} with key {msg.key().decode('utf-8')} with value {msg.value().decode('utf-8')}")

    def send(self, value:dict, key:str=None):
        """ Send a new event to the sink topic. 
        If one does NOT provide a key, we read the config file for the path of the key. 
        If there is no key defined (which would slow the system down) we omit adding a key. """

        if key is None:
            # Read the config.ini
            key_path = self.get_key_path()
            json_path_expr = parse(key_path)
            try:
                key = [match.value for match in json_path_expr.find(value)][0]
            except Exception as ex:
                logger.info(f"No key matched the parse expression ({json_path_expr}). Thus the key ({key}) is not modified.")
            logger.debug(f"The key is set to {key}.")

        # TODO @Julian, Hier muss du noch die SchemaRegistry eintragen --> config.ini
        # Siehe Beispiel: https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/avro_producer.py
        # AVRO serialization if needed.
        # path = os.path.realpath(os.path.dirname(__file__))
        # with open(f"{path}/avro/{schema}") as f:
        #     schema_str = f.read()
        # schema_registry_conf = {'url': args.schema_registry}
        # schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        # avro_serializer = AvroSerializer(schema_registry_client, schema_str, user_to_dict)


        new_event = json.dumps(value)
        logger.debug(f"Event production: {value} with key {key}")
        try: 
            self.producer.produce(self.topic, new_event.encode('utf-8'), key.encode('utf-8'), callback=self.delivery_callback)
        except Exception as ex:
            logger.error(f"Failed to product an event to self.topic for the value {value} und key {key}. The error is: {ex}")
        #self.producer.produce(self.topic, avro_serializer(user, SerializationContext(self.topic, MessageField.VALUE)), key.encode('utf-8'), callback=self.delivery_callback)
        self.producer.poll(0)
        self.producer.flush()