from streamsx.topology.topology import Topology
from streamsx.topology.context import submit, ContextTypes
from streamsx.topology.topology import Routing
from streamsx.topology.schema import StreamSchema
from streamsx.kafka import KafkaConsumer, KafkaProducer
from streamsx.kafka.schema import Schema

import random
import time
import json
from datetime import datetime


# Define a callable source for data that we push into Event Streams
class SensorReadingsSource(object):
    def __call__(self):
        # This is just an example of using generated data,
        # Here you could connect to db
        # generate data
        # connect to data set
        # open file
        i = 0
        # wait that the consumer is ready before we start creating data
        time.sleep(20.0)
        while(i < 100000):
            time.sleep(0.001)
            i = i + 1
            sensor_id = random.randint(1, 100)
            reading = {}
            reading["sensor_id"] = "sensor_" + str(sensor_id)
            reading["value"] = random.random() * 3000
            reading["ts"] = int(datetime.now().timestamp())
            yield reading


# parses the JSON in the message and adds the attributes to a tuple
def flat_message_json(tuple):
    messageAsDict = json.loads(tuple['message'])
    tuple.update(messageAsDict)
    return tuple


# calculate a hash code of a string in a consistent way
# needed for partitioned parallel streams
def string_hashcode(s):
    h = 0
    for c in s:
        h = (31 * h + ord(c)) & 0xFFFFFFFF
    return ((h + 0x80000000) & 0xFFFFFFFF) - 0x80000000


topology = Topology('KafkaGroupParallel')
kafka_topic = 'THREE_PARTITION_TOPIC'
#
# the producer part
#
# create the data and map them to the attributes 'message' and 'key' of the
# 'Schema.StringMessage' schema for Kafka, so that we have messages with keys
sensorStream = topology.source(
    SensorReadingsSource(),
    "RawDataSource"
    ).map(
        func=lambda reading: {'message': json.dumps(reading),
                              'key': reading['sensor_id']},
        name="ToKeyedMessage",
        schema=Schema.StringMessage)
# assume, we are running a Kafka broker at localhost:9092
producer_config = dict()
producer_config['bootstrap.servers'] = 'localhost:9092'
producer = KafkaProducer(config=producer_config, topic=kafka_topic)
sensorStream.for_each(producer, name="SensorPublish")
#
# the consumer side
#
# subscribe, create a consumer group with 3 consumers
consumer_config = dict()
consumer_config['bootstrap.servers'] = 'localhost:9092'

consumerSchema = Schema.StringMessageMeta
consumer = KafkaConsumer(config=consumer_config,topic=kafka_topic,schema=consumerSchema)
consumer.group_id = 'my_consumer_group'
consumer.group_size = 3
received = topology.source(consumer, name="SensorSubscribe").end_parallel()

# start a different parallel region partitioned by message key,
# so that each key always goes into the same parallel channel
receivedParallelPartitioned = received.parallel(
    5,
    routing=Routing.HASH_PARTITIONED,
    func=lambda x: string_hashcode(x['key']))

# schema extension, here we use the Python 2.7, 3 way
flattenedSchema = consumerSchema.extend(
    StreamSchema('tuple<rstring sensor_id, float64 value, int64 ts>'))

receivedParallelPartitionedFlattened = receivedParallelPartitioned.map(
    func=flat_message_json,
    name='JSON2Attributes',
    schema=flattenedSchema)

# validate by remove negativ and zero values from the streams,
# pass only positive vaues and timestamps
receivedValidated = receivedParallelPartitionedFlattened.filter(
    lambda tup: (tup['value'] > 0) and (tup['ts'] > 0),
    name='Validate')

# end parallel processing and print the merged result stream to stdout log
receivedValidated.end_parallel().print()

submit(ContextTypes.DISTRIBUTED, topology)
