import collections
import json
import logging

from confluent_kafka import Producer, Consumer

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
            'transactional.id': 'ethereumetl',
            'enable.idempotence': True,
        })

        self.consumer = Consumer({
            'bootstrap.servers': self.connection_url,
            'group.id': 'ethereumetl',
            'auto.offset.reset': 'largest',
        })

    def get_last_synced_block(self):
        self.consumer.subscribe([f'{self.topic_prefix}blocks'])
        message = self.consumer.consume()
        try:
            print(json.loads(message.value())['number'])
            print(int(json.loads(message.value())['number']))
            return int(json.loads(message.value())['number'])
        except:
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
        self.producer.commit_transaction()
        
    def export_item(self, item):
        item_type = item.get('type')
        if item_type is not None and item_type in self.item_type_to_topic_mapping:
            data = json.dumps(item).encode('utf-8')
            logging.debug(data)
            return self.producer.produce(self.topic_prefix + self.item_type_to_topic_mapping[item_type], data)
        else:
            logging.warning('Topic for item type "{}" is not configured.'.format(item_type))

    def convert_items(self, items):
        for item in items:
            yield self.converter.convert_item(item)

    def close(self):
       pass
        
def group_by_item_type(items):
    result = collections.defaultdict(list)
    for item in items:
        result[item.get('type')].append(item)

    return result
