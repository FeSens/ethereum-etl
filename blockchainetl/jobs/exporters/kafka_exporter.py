import collections
import json
import logging
from hashlib import sha256

from confluent_kafka import Producer, Consumer, TopicPartition

from blockchainetl.jobs.exporters.converters.composite_item_converter import CompositeItemConverter


class KafkaItemExporter:

    def __init__(self, output, item_type_to_topic_mapping, converters=()):
        self.item_type_to_topic_mapping = item_type_to_topic_mapping
        self.converter = CompositeItemConverter(converters)
        self.connection_url = self.get_connection_url(output)
        self.topic_prefix = self.get_topic_prefix(output)
        print(self.connection_url, self.topic_prefix)
        self.producer = Producer({
            'bootstrap.servers': self.connection_url,
            'transactional.id': f'ethereumetl-producer-{self.topic_prefix}',
            'enable.idempotence': True,
            'message.max.bytes': 16000012,
        })

        self.consumer = Consumer({
            'bootstrap.servers': self.connection_url,
            'group.id': 'ethereumetl-consumer',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'session.timeout.ms': 6000,
            'max.poll.interval.ms': 6000,
            'isolation.level': 'read_committed',
        })

    def get_last_synced_block(self):
        try:
            lag = 10
            topic_name = f'{self.topic_prefix}blocks'
            topic = self.consumer.list_topics(topic=topic_name)
            partitions = [TopicPartition(topic_name, partition) for partition in list(topic.topics[topic_name].partitions.keys())] 
            offsets = [self.consumer.get_watermark_offsets(partition)[-1] for partition in partitions]
            topic_get_last_committed_offset =  [TopicPartition(topic_name, partition, max(offset - lag, 0)) for partition, offset in zip(list(topic.topics[topic_name].partitions.keys()), offsets)]
            
            self.consumer.assign(topic_get_last_committed_offset)

            messages = self.consumer.consume(num_messages=len(partitions)*lag, timeout=10)
            self.consumer.unsubscribe()
            last_block_number = int(max(messages, key=lambda m: int(m.key())).key())
            logging.debug(f"Last block synced to Kafka was: {last_block_number}")

            return last_block_number

        except Exception as e:
            print(e)
            return 0


    def get_connection_url(self, output):
        try:
            return output.split('/')[1]
        except IndexError:
            raise Exception('Invalid kafka output param, It should be in format of "kafka/127.0.0.1:9092" or "kafka/127.0.0.1:9092/<topic-prefix>"')

    def get_topic_prefix(self, output):
        try:
            return output.split('/')[2] + "."
        except IndexError:
            return ''

    def open(self):
        self.producer.init_transactions()

    def export_items(self, items):
        self.producer.begin_transaction()
        
        for item in items:
            self.export_item(item)
        
        # self.producer.produce(self.topic_prefix + "full_block", value=self.pack_full_block(items))
        
        self.producer.commit_transaction()
        
    def export_item(self, item):
        item_type = item.get('type')
        key = item.get('number') or sha256(json.dumps(item).encode('utf-8')).hexdigest()
        if item_type is not None and item_type in self.item_type_to_topic_mapping:
            data = json.dumps(item).encode('utf-8')
            logging.debug(data)
            return self.producer.produce(self.topic_prefix + self.item_type_to_topic_mapping[item_type], value=data, key=json.dumps(key).encode('utf-8'))
        else:
            logging.warning('Topic for item type "{}" is not configured.'.format(item_type))

    def convert_items(self, items):
        for item in items:
            yield self.converter.convert_item(item)

    def close(self):
       pass

    def pack_full_block(self, items):
        full_block = {}
        for item in items:
            item_type = self.item_type_to_topic_mapping[item.get('type')]
            
            if item_type is not None and item_type in ["blocks", "logs", "transactions", "token_transfers"]:
                if item_type not in full_block:
                    full_block[item_type] = []
                full_block[item_type].append(item)
        
        encoded_message = json.dumps(full_block).encode('utf-8')
        logging.info(f"Trying to produce a message of size: {len(encoded_message)} bytes")
        return encoded_message
        
def group_by_item_type(items):
    result = collections.defaultdict(list)
    for item in items:
        result[item.get('type')].append(item)

    return result
