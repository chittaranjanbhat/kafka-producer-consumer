import json
import random


from kafka import KafkaProducer
from kafka.errors import KafkaError

def produce_data(json_data,key):
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                             key_serializer=str.encode)

    future = producer.send('test', value=json_data,key=key)

    # Block for 'synchronous' sends
    try:
        record_metadata = future.get(timeout=10)
        on_send_success(record_metadata)
    except KafkaError:
        # Decide what to do if produce request failed...
        on_send_error(KafkaError)
        pass

    producer.flush()

def on_send_success(record_metadata):
    print ("Topic:%s: Part:%d: Offset:%d" % (record_metadata.topic, record_metadata.partition,
                                          record_metadata.offset))

def on_send_error(excp):
    print('I am an errback', exc_info=excp)


if __name__ == '__main__':
    print('Producing data now..!')
    data = {1: 'Geeks', 2: 'For', 3: 'Geeks'}
    data1 = {'key': 'valueeee'}

    data = {
  "firstName": "Rack",
  "lastName": "Jackon",
  "gender": "man",
  "age": 24,
  "address": {
    "streetAddress": "126",
    "city": "San Jone",
    "state": "CA",
    "postalCode": "394221"
  },
  "phoneNumbers": [
    {
      "type": "home",
      "number": "7383627627"
    }
  ]
}
    key = random.randint(1000,20000)
    for x in range(1):
        produce_data(data,str(key))