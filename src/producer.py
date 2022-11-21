import sys
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING, Producer as KProducer
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
    

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(self, err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    def send(self, key:str, value:str):
        # TODO @Julian, Hier muss du noch die SchemaRegistry eintragen --> config.ini
        # Siehe Beispiel: https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/avro_producer.py
        # AVRO serialization if needed.
        # path = os.path.realpath(os.path.dirname(__file__))
        # with open(f"{path}/avro/{schema}") as f:
        #     schema_str = f.read()
        # schema_registry_conf = {'url': args.schema_registry}
        # schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        # avro_serializer = AvroSerializer(schema_registry_client, schema_str, user_to_dict)

        self.producer.produce(self.topic, value.encode('utf-8'), key.encode('utf-8'), callback=self.delivery_callback)
        #self.producer.produce(self.topic, avro_serializer(user, SerializationContext(self.topic, MessageField.VALUE)), key.encode('utf-8'), callback=self.delivery_callback)
        self.producer.poll(0)
        self.producer.flush()