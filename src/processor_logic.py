import json
import sys
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING, Producer
from consumer_factory import ConsumerFactory
from producer import Producer as MyProducer
import globallogger

logger = globallogger.setup_custom_logger('app')

class ProcessorLogic:
    """This event processor implements the splitter pattern. https://www.enterpriseintegrationpatterns.com/patterns/messaging/Sequencer.html"""

    def __init__(self):
        self.consumer_factory = ConsumerFactory()
        self.poll_interval = self.consumer_factory.get_poll_interval()
        self.consumer = self.consumer_factory.get_subscribed_consumer()  
        self.name_of_array_field = self.consumer_factory.get_name_of_array_field()
        self.producer = MyProducer()

    def execute(self):                
        # Poll for new messages from Kafka and print them.
        try:
            while True:
                msg = self.consumer.poll(self.poll_interval)
                if msg is None:                
                    logger.debug(f"Listening ...")
                elif msg.error():
                    logger.error("ERROR: %s".format(msg.error()))
                else:
                    logger.debug("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                        topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
                    # Unpack the batch of the array and submit them one by one to the producer topic   
                    messages = json.loads(msg.value().decode('utf-8'))                    
                    for event in messages[self.name_of_array_field]:
                        self.produce_event(event)                    
        except KeyboardInterrupt:
            pass
        finally:
            logger.info("Shutting down the consumer, leaving the consumer-group and comitting the final offset.")
            self.consumer.close()

    def produce_event(self, event: dict):
        logger.info(event)
        try:
            #@Julian, depending on the key you want, you need to set it here. e.g. AHV-Nr.
            self.producer.send(key="any_key", value=str(event))
        except Exception as ex:
            logger.error(f"Failed to produce event. {ex}. {event}. Shutting down the application to avoid data loss.")
            sys.exit()