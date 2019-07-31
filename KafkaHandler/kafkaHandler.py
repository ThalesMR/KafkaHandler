import json

from confluent_kafka import Producer, Consumer, KafkaError


# The best way to use this class is creating a KafkaHandler object like:
#   handler = KafkaHandler("<YOUR_KAFKA_SERVER_STRING>")
#   handler.set_producer()
#   handler.set_topic("<YOUR_TOPIC>")
#   handler.send_message("<YOUR_MESSAGE>")
# And to Consumer is:
#   handler = KafkaHandler("<YOUR_KAFKA_SERVER_STRING>")
#   handler.set_consumer("<YOUR_GROUP_ID>")
#   handler.add_topic("<YOUR_TOPIC>") or You can add many topics you want
#   handler.consumer_subscribe()
#   consumer = handler.get_consumer()
#   while true:
#       msg = consumer.poll(0.1)
#
class KafkaHandler:
    #
    #
    #
    def __init__(self, broker_list):
        self._brokers = broker_list
        self._producer = None
        self._consumer = None
        self._topic = None
        self._topics = []

    #
    # Method used to set a single topic for producer.
    #

    def set_topic(self, topic):
        self._topic = topic

    #
    # Method used to add a new topic for the consumer.
    #

    def add_topic(self, topic):
        self._topics.append(topic)

    #
    # Method used for remove a topic from the List of topics that a consumer is using.
    #

    def remove_topic(self, topic):
        if not self._topics:
            self._topics.remove(topic)

    #
    # Initialize the producer.
    #

    def set_producer(self):
        self._producer = Producer({'bootstrap.servers': self._brokers})

    #
    # Initialize the consumer.
    #

    def set_consumer(self, group_id):
        try:
            self._consumer = Consumer({
                'bootstrap.servers': self._brokers,
                'group.id': group_id,
                'auto.offset.reset': 'earliest'
            })
        except Exception as ex:
            print("Exception on creating  consumer.")
            raise ex

    #
    # Close consumer connection.
    #
    def consumer_close(self):
        self._consumer.close()

    #
    # Subscribe a Consumer to the Topics
    #
    def consumer_subscribe(self):
        self._consumer.subscribe(self._topics)

    #
    # Get the Consumer Object.
    #
    def get_consumer(self):
        return self._consumer

    #
    # A callback function to format the message.
    #
    def _delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    #
    # Send Message to Kafka Queue.
    #
    def send_message(self, message):
        # Trigger any available delivery report callbacks from previous produce() calls
        self._producer.poll(0)

        # Asynchronously produce a message, the delivery report callback
        # will be triggered from poll() above, or flush() below, when the message has
        # been successfully delivered or failed permanently.
        self._producer.produce(self._topic, message, callback=self._delivery_report)
        self._producer.flush()
