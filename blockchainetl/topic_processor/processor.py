from confluent_kafka import Producer, Consumer, TopicPartition
import json

def connect_kafka_consumer(kafkaUrl, kafkaTopic):
  _consumer = None
  try:
    _consumer = Consumer({
        'bootstrap.servers': kafkaUrl,
        'group.id': 'evm-etl',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    })
    _consumer.subscribe([kafkaTopic])
  except Exception as e:
    print('Exception while connecting Kafka')
    print(str(e))
  finally:
    return _consumer

def get_one_message(consumer):
  message = consumer.poll(1.0)
  if message is None:
    print('No message received by consumer')
  elif message.error() is not None:
    print('Error received from consumer')
  else:
    print('Message received: {}'.format(message.value().decode('utf-8')))
  
  return json.loads(message)

def commit_message(consumer, message):
  try:
    consumer.commit(message=message)
  except Exception as e:
    print('Commit failed')
    print(str(e))
